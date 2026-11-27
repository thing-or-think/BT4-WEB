package croc

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/schollz/croc/v11/src/bundle"
	"github.com/schollz/croc/v11/src/message"
)

// SyncRounds lets a SyncHook run several sync rounds over one encrypted session.
type SyncRounds interface {
	// Wake fires when the hook wants the control loop to call OnWake, for example
	// when the source has new changes or a keepalive is due. It may return nil.
	Wake() <-chan struct{}
	// OnWake runs on the control goroutine and may send sync messages.
	OnWake(send func(message.Message) error) error
	// KeepOpen is asked after each round's finished exchange. true keeps the session
	// open: croc resets per-round state and waits for the next round.
	KeepOpen() bool
	// NextFiles is called on the sender when a new round starts (the hook became
	// Ready again after a reset) and returns that round's complete candidate list.
	NextFiles() (files, emptyFolders []FileInfo, totalFolders int)
}

func (c *Client) syncWakeChan() <-chan struct{} {
	hook := c.Options.SyncHook
	if hook == nil || !c.sync.started || !c.lifecycleSnapshot().ChannelSecured {
		return nil
	}
	rounds, ok := hook.(SyncRounds)
	if !ok {
		return nil
	}
	return rounds.Wake()
}

func (c *Client) handleSyncWake(attempt *transferAttemptState) error {
	rounds, ok := c.Options.SyncHook.(SyncRounds)
	if !ok {
		return nil
	}
	if err := rounds.OnWake(c.syncSend); err != nil {
		return err
	}
	return c.updateState(attempt)
}

func (c *Client) keepSyncSessionOpen() bool {
	rounds, ok := c.Options.SyncHook.(SyncRounds)
	if !ok {
		return false
	}
	return rounds.KeepOpen()
}

// startSenderFilePreparation runs sender-side file preparation in the background.
// When a SyncHook is configured, it waits for the hook's selection on c.sync.files
// before hashing and probing compression, then signals c.filesReady.
func (c *Client) startSenderFilePreparation(filesInfo []FileInfo) <-chan error {
	hashResult := make(chan error, 1)
	var syncFiles <-chan syncSelection
	var prepDone chan struct{}
	if c.Options.SyncHook != nil {
		ch := make(chan syncSelection, 1)
		c.sync.files = ch
		syncFiles = ch
		prepDone = make(chan struct{})
		c.sync.prepDone = prepDone
	}
	go func() {
		if prepDone != nil {
			defer close(prepDone)
		}
		if syncFiles != nil {
			// transferBytes above is the upper bound used for transport choice;
			// only the files the hook keeps are hashed and sent.
			select {
			case selection := <-syncFiles:
				filesInfo = selection.files
				c.EmptyFoldersToTransfer = selection.emptyFolders
				c.TotalNumberFolders = selection.totalFolders
				c.TotalNumberOfContents = len(filesInfo)
			case <-c.stop.ctx.Done():
				c.finishFilePreparation(c.stop.ctx.Err())
				hashResult <- c.stop.ctx.Err()
				return
			}
		}
		prepareErr := c.sendCollectFiles(filesInfo)
		c.finishFilePreparation(prepareErr)
		hashResult <- prepareErr
	}()
	return hashResult
}

// resetSyncRound clears per-round transfer state while preserving the open
// connections, session encryption key, PAKE negotiation, negotiated features,
// selected data transport (Tailcat bundle or relay channels), and relay standby.
func (c *Client) resetSyncRound() error {
	// Ensure any sender data workers from the completed round have exited.
	c.joinSenderUnit()
	c.senderDataMu.Lock()
	c.senderDataWorkers = nil
	c.senderWorkerSequence = 0
	c.senderDataMu.Unlock()
	if c.fread != nil {
		_ = c.fread.Close()
		c.fread = nil
	}

	if c.receiveMutex != nil {
		c.receiveMutex.Lock()
	}
	if c.CurrentFile != nil && !c.CurrentFileIsClosed {
		_ = c.CurrentFile.Close()
	}
	c.CurrentFile = nil
	c.CurrentFileIsClosed = false
	c.CurrentFileChunkRanges = nil
	c.CurrentFileChunkCount = 0
	c.TotalSent = 0
	c.TotalChunksTransferred = 0
	if c.receiveMutex != nil {
		c.receiveMutex.Unlock()
	}

	cleanupErr := errors.Join(c.removeBundleStage(), c.removeBundleSpool())
	c.bundleCommitted = nil
	c.bundles.identities = nil
	c.bundles.manifest = false
	c.bundles.manifestNative = false
	c.bundles.peerLimits = bundle.Limits{}
	s := c.bundleState()
	s.receive.Store(nil)
	s.lastClosed = time.Time{}
	s.readyAt = time.Time{}
	s.lifecycle.ActiveID = 0
	s.lifecycle.Phase = ""
	s.request = bundle.Request{}
	s.offsets = nil
	s.size = 0
	s.offer = bundle.Offer{}
	s.publication = nil

	c.resetRoundLifecycle()
	c.tailcat.terminal.Store(false)
	// Mark the session active so a peer disconnect while waiting between rounds
	// always surfaces as a transferDisconnectError, even after an empty round.
	c.markTransferStarted()

	c.FilesToTransfer = nil
	c.EmptyFoldersToTransfer = nil
	c.TotalNumberOfContents = 0
	c.TotalNumberFolders = 0
	c.FilesToTransferCurrentNum = 0
	c.FilesHasFinished = make(map[int]struct{})
	c.TotalFilesIgnored = 0
	c.LastFolder = ""
	c.numfinished = 0
	c.finishedNum = 0
	c.numberOfTransferredFiles = 0
	c.numberOfUnchangedFiles = 0
	c.filesWrittenByReceiver = nil
	c.longestFilename = 0
	c.firstSend = false

	c.clearIncomingFileManifest()
	c.receiveChoices = nil

	if display := c.progressDisplaySnapshot(); display != nil {
		display.close(true)
	}
	c.barMu.Lock()
	c.progressDisplay = nil
	c.bar = nil
	c.barMu.Unlock()

	c.exactHashMu.Lock()
	c.exactHashPending = -1
	c.exactHashLocal = nil
	c.exactHashResults = make(map[int]bool)
	c.exactHashMu.Unlock()

	c.sync.planned = false
	c.sync.finished = false
	c.sync.round++

	if c.Options.IsSender {
		// Wait for the previous round's preparation goroutine to finish before
		// replacing filesReady and filesReadyOnce.
		if c.sync.prepDone != nil {
			select {
			case <-c.sync.prepDone:
			case <-c.stop.ctx.Done():
				return c.stop.ctx.Err()
			}
			c.sync.prepDone = nil
		}
		c.preparedHashAlgorithm = ""
		c.sourceSnapshots = nil
		c.remainingPreparationOnce = sync.Once{}
		c.preparationErr = atomic.Value{}
		c.resetFilePreparation()
		c.startSenderFilePreparation(nil)
	}

	return cleanupErr
}
