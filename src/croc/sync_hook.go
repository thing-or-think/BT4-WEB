package croc

import (
	"errors"
	"fmt"
	"strings"

	"github.com/schollz/croc/v11/src/message"
)

// SyncMessagePrefix marks control messages owned by a SyncHook. Core croc
// never interprets them; it only routes them once the channel is secured.
const SyncMessagePrefix = "sync-"

// SyncHook lets a higher layer (src/foldersync) exchange its own encrypted
// control messages after PAKE and before file metadata, and choose which of the
// files passed to Send are actually transferred. croc must not import that
// layer, so the dependency points this way.
//
// All methods run on the control goroutine; implementations need no locking
// for state touched only from these callbacks.
type SyncHook interface {
	// Feature is advertised during PAKE. Both peers must advertise it.
	Feature() string
	// Start runs once on each peer when the encrypted channel is ready.
	Start(send func(message.Message) error) error
	// Handle receives every message whose type starts with SyncMessagePrefix.
	Handle(m message.Message, send func(message.Message) error) error
	// Ready is polled on the sender after each control message. Once it
	// reports true, keep selects the files and empty folders to send.
	Ready() (ready bool, keep func(FileInfo) bool)
}

// ErrPeerLacksSync reports a peer that did not negotiate the hook's feature.
var ErrPeerLacksSync = errors.New("peer does not support folder sync")

type syncHookState struct {
	peerFeature bool
	started     bool
	planned     bool
	// files receives the sender's selected list exactly once.
	files chan syncSelection
}

type syncSelection struct {
	files, emptyFolders []FileInfo
}

func (c *Client) syncSend(m message.Message) error {
	return message.Send(c.connection(0), c.Key, m)
}

func isSyncMessage(t message.Type) bool {
	return strings.HasPrefix(string(t), SyncMessagePrefix)
}

// handleSyncMessage routes a hook message. It is reached only after the
// message decoded with the session key, so it is authenticated.
func (c *Client) handleSyncMessage(m message.Message) error {
	hook := c.Options.SyncHook
	if hook == nil {
		return fmt.Errorf("unexpected %s message: folder sync was not requested", m.Type)
	}
	if !c.sync.started {
		return fmt.Errorf("%s message before folder sync started", m.Type)
	}
	return hook.Handle(m, c.syncSend)
}

// updateSyncHook starts the hook when the channel becomes secure and, on the
// sender, releases file preparation once the hook has chosen what to send.
func (c *Client) updateSyncHook() error {
	hook := c.Options.SyncHook
	if hook == nil || !c.lifecycleSnapshot().ChannelSecured {
		return nil
	}
	if !c.sync.started {
		if !c.sync.peerFeature {
			return fmt.Errorf("%w (%s); update croc on both computers and run croc sync on each", ErrPeerLacksSync, hook.Feature())
		}
		c.sync.started = true
		if err := hook.Start(c.syncSend); err != nil {
			return err
		}
	}
	if !c.Options.IsSender || c.sync.planned {
		return nil
	}
	ready, keep := hook.Ready()
	if !ready {
		return nil
	}
	c.sync.planned = true
	c.sync.files <- syncSelection{
		files:        filterFileInfos(c.FilesToTransfer, keep),
		emptyFolders: filterFileInfos(c.EmptyFoldersToTransfer, keep),
	}
	return nil
}

func filterFileInfos(infos []FileInfo, keep func(FileInfo) bool) []FileInfo {
	if keep == nil {
		return infos
	}
	var kept []FileInfo
	for _, info := range infos {
		if keep(info) {
			kept = append(kept, info)
		}
	}
	return kept
}
