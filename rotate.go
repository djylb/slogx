package slogx

import (
	"compress/gzip"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Rotation is a log file and when to rotate it. Zero MaxSize means 64 MiB;
// negative means never rotate by size. Every, when positive, also rotates a
// file that has been written for that long. Zero MaxFiles and MaxAge keep
// every rotated file.
type Rotation struct {
	Path string
	// MaxSize is the size in bytes at which the file rotates.
	MaxSize int64
	// Every is the age at which the file rotates.
	Every time.Duration
	// MaxFiles is how many rotated files are kept, newest first.
	MaxFiles int
	// MaxAge is how long rotated files are kept, by the time in their name.
	MaxAge time.Duration
	// BufferSize, when positive, collects lines in a buffer of that many
	// bytes and writes them to the file together, instead of one write
	// per line. The buffer is written out when the next line does not fit,
	// FlushInterval after its first line, on Flush, Reopen, rotation and
	// Close. Lines still in the buffer are lost if the process dies
	// without Close. Zero or negative writes every line at once.
	BufferSize int
	// FlushInterval bounds how long a buffered line waits for the file;
	// zero is one second, negative never flushes on time (only on size,
	// Flush, Reopen, rotation and Close). Unused without BufferSize.
	FlushInterval time.Duration
	// Compress gzips rotated files.
	Compress bool
	// Mode is the permission of new log files; zero is 0640, readable by
	// the owner's group (such as an admin group running fail2ban) and no
	// one else. Directories made for them get the same bits plus search.
	Mode os.FileMode
}

func (c Rotation) mode() os.FileMode {
	if c.Mode == 0 {
		return 0o640
	}
	return c.Mode & os.ModePerm
}

func dirMode(m os.FileMode) os.FileMode { return m | (m&0o444)>>2 }

const defaultMaxSize = 64 << 20

// rotatedLayout is the time in a rotated file's name, UTC:
// <name>-20261108T031245.000[-N]<ext>[.gz].
const rotatedLayout = "20060102T150405.000"

// Rotating is an Output to a file that rotates itself: the full file is
// renamed to <name>-<UTC time><ext> and a new one started. Compression and
// pruning of rotated files run in order on one goroutine of its own, so they
// never race each other or the writer.
type Rotating struct {
	f       *os.File
	work    chan struct{} // a rotation happened: compress and prune
	timer   *time.Timer   // writes buf out FlushInterval after its first line
	buf     []byte        // lines not yet written, with BufferSize
	late    error         // a timed flush failed; the next call returns it
	dir     string
	base    string // file name without ext
	ext     string
	cfg     Rotation
	opened  time.Time
	size    int64
	workers sync.WaitGroup
	mu      sync.Mutex
	closed  bool
}

const defaultFlushInterval = time.Second

// OpenRotating opens the file of c, creating its directory.
func OpenRotating(c Rotation) (*Rotating, error) {
	if c.MaxSize == 0 {
		c.MaxSize = defaultMaxSize
	}
	if err := os.MkdirAll(filepath.Dir(c.Path), dirMode(c.mode())); err != nil {
		return nil, err
	}
	name := filepath.Base(c.Path)
	ext := filepath.Ext(name)
	r := &Rotating{cfg: c, dir: filepath.Dir(c.Path), base: strings.TrimSuffix(name, ext), ext: ext, work: make(chan struct{}, 1)}
	if err := r.open(); err != nil {
		return nil, err
	}
	if c.BufferSize > 0 {
		r.buf = make([]byte, 0, c.BufferSize)
	}
	r.workers.Go(r.maintain)
	return r, nil
}

func (r *Rotating) open() error {
	f, err := os.OpenFile(r.cfg.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, r.cfg.mode())
	if err != nil {
		return err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	r.f, r.size, r.opened = f, fi.Size(), time.Now()
	return nil
}

// Write appends p to the file, or to the buffer with BufferSize, rotating
// the file first when p would take it past MaxSize or the file is older
// than Every. With a buffer, the error of writing earlier lines out,
// including by the FlushInterval timer, is returned by the next Write,
// Flush or Close.
func (r *Rotating) Write(p []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return errors.Join(r.takeLate(), r.write(p))
}

// takeLate returns and clears the error of a timed flush; r.mu is held.
func (r *Rotating) takeLate() error {
	err := r.late
	r.late = nil
	return err
}

func (r *Rotating) write(p []byte) error {
	if r.closed {
		return os.ErrClosed
	}
	if r.f == nil {
		if err := r.open(); err != nil {
			return err
		}
	}
	var err error
	bySize := r.cfg.MaxSize > 0 && r.size > 0 && r.size+int64(len(p)) > r.cfg.MaxSize
	byTime := r.cfg.Every > 0 && r.size > 0 && time.Since(r.opened) >= r.cfg.Every
	if bySize || byTime {
		// lines buffered for the old file go to it, not the new one
		err = r.writeBuffered()
		r.rotate()
		if r.f == nil {
			if oerr := r.open(); oerr != nil {
				return errors.Join(err, oerr)
			}
		}
	}
	if r.cfg.BufferSize <= 0 {
		n, werr := r.f.Write(p)
		r.size += int64(n)
		return errors.Join(err, werr)
	}
	if len(r.buf)+len(p) > cap(r.buf) {
		err = errors.Join(err, r.writeBuffered())
	}
	if len(p) >= cap(r.buf) {
		n, werr := r.f.Write(p)
		r.size += int64(n)
		return errors.Join(err, werr)
	}
	if len(r.buf) == 0 {
		r.armTimer()
	}
	r.buf = append(r.buf, p...)
	r.size += int64(len(p))
	return err
}

// writeBuffered writes the buffered lines to the file; r.mu is held. The
// buffer is emptied even when the write fails, so a broken file cannot
// grow it without bound.
func (r *Rotating) writeBuffered() error {
	if len(r.buf) == 0 {
		return nil
	}
	if r.timer != nil {
		r.timer.Stop()
	}
	var err error
	if r.f != nil {
		_, err = r.f.Write(r.buf)
	} else {
		err = os.ErrClosed
	}
	r.buf = r.buf[:0]
	return err
}

// armTimer schedules writing out the buffer that a line is about to start;
// r.mu is held.
func (r *Rotating) armTimer() {
	d := r.cfg.FlushInterval
	if d < 0 {
		return
	}
	if d == 0 {
		d = defaultFlushInterval
	}
	if r.timer == nil {
		r.timer = time.AfterFunc(d, r.flushOnTime)
		return
	}
	r.timer.Reset(d)
}

// flushOnTime writes out the buffer when FlushInterval has passed. Its
// error has no caller, so it is kept for the next Write, Flush or Close.
func (r *Rotating) flushOnTime() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		r.late = errors.Join(r.late, r.writeBuffered())
	}
}

// rotate renames the current file to a name no other rotated file has and
// starts a new one; r.mu is held and the buffer written out.
func (r *Rotating) rotate() {
	_ = r.f.Close()
	r.f = nil
	stamp := time.Now().UTC().Format(rotatedLayout)
	var name string
	for i := 0; ; i++ {
		suffix := ""
		if i > 0 {
			suffix = "-" + strconv.Itoa(i)
		}
		name = filepath.Join(r.dir, r.base+"-"+stamp+suffix+r.ext)
		if !exists(name) && !exists(name+".gz") {
			break
		}
	}
	if os.Rename(r.cfg.Path, name) == nil {
		select {
		case r.work <- struct{}{}:
		default: // a pass is already due; it sees this file too
		}
	}
	_ = r.open()
}

func exists(name string) bool {
	_, err := os.Lstat(name)
	return err == nil
}

// maintain prunes and then compresses the rotated files after each
// rotation. Each pass looks at every rotated file, so rotations that come
// faster than a pass are never left behind.
func (r *Rotating) maintain() {
	for range r.work {
		r.prune()
		if r.cfg.Compress {
			for _, f := range r.rotatedFiles() {
				if !strings.HasSuffix(f.path, ".gz") {
					compressFile(f.path, r.cfg.mode())
				}
			}
		}
	}
}

// Reopen closes and reopens the file, after an external rotation.
func (r *Rotating) Reopen() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return os.ErrClosed
	}
	err := r.writeBuffered()
	if r.f != nil {
		_ = r.f.Close()
	}
	r.f = nil
	return errors.Join(err, r.open())
}

// Flush writes out the buffer and syncs the file to disk.
func (r *Rotating) Flush() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	err := errors.Join(r.takeLate(), r.writeBuffered())
	if r.f != nil {
		err = errors.Join(err, r.f.Sync())
	}
	return err
}

// Close closes the file and waits for compression and pruning; writes
// after it fail.
func (r *Rotating) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	err := errors.Join(r.takeLate(), r.writeBuffered())
	if r.timer != nil {
		r.timer.Stop()
	}
	if r.f != nil {
		err = errors.Join(err, r.f.Close())
		r.f = nil
	}
	close(r.work)
	r.mu.Unlock()
	r.workers.Wait()
	return err
}

// rotated is a file that Rotating made, with the time in its name.
type rotated struct {
	path string
	at   time.Time
	seq  int
}

// rotatedFiles lists the files of the directory whose names are exactly
// what rotate produces for this file, newest first. Other files, such as a
// sibling log whose name starts the same, are never listed.
func (r *Rotating) rotatedFiles() []rotated {
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		return nil
	}
	var out []rotated
	prefix := r.base + "-"
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !strings.HasPrefix(n, prefix) {
			continue
		}
		rest := strings.TrimSuffix(n[len(prefix):], ".gz")
		if !strings.HasSuffix(rest, r.ext) {
			continue
		}
		rest = rest[:len(rest)-len(r.ext)]
		if len(rest) < len(rotatedLayout) {
			continue
		}
		at, err := time.Parse(rotatedLayout, rest[:len(rotatedLayout)])
		if err != nil {
			continue
		}
		seq := 0
		if tail := rest[len(rotatedLayout):]; tail != "" {
			if tail[0] != '-' {
				continue
			}
			if seq, err = strconv.Atoi(tail[1:]); err != nil || seq < 1 {
				continue
			}
		}
		out = append(out, rotated{path: filepath.Join(r.dir, n), at: at, seq: seq})
	}
	slices.SortFunc(out, func(a, b rotated) int {
		if c := b.at.Compare(a.at); c != 0 {
			return c
		}
		return b.seq - a.seq
	})
	return out
}

// prune removes rotated files beyond MaxFiles or older than MaxAge. A file
// and its compressed copy (while compression runs) count once.
func (r *Rotating) prune() {
	if r.cfg.MaxFiles <= 0 && r.cfg.MaxAge <= 0 {
		return
	}
	kept := 0
	seen := map[string]bool{}
	for _, f := range r.rotatedFiles() {
		key := strings.TrimSuffix(f.path, ".gz")
		if seen[key] {
			continue
		}
		seen[key] = true
		drop := r.cfg.MaxFiles > 0 && kept >= r.cfg.MaxFiles
		if !drop && r.cfg.MaxAge > 0 && time.Since(f.at) > r.cfg.MaxAge {
			drop = true
		}
		if drop {
			_ = os.Remove(key)
			_ = os.Remove(key + ".gz")
			continue
		}
		kept++
	}
}

// compressFile writes name.gz through a temporary file, then removes name.
func compressFile(name string, mode os.FileMode) {
	in, err := os.Open(name) //nolint:gosec // name is a rotated file of this package, not input
	if err != nil {
		return
	}
	defer func() { _ = in.Close() }()
	tmp := name + ".gz.tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_EXCL, mode) //nolint:gosec // tmp derives from the rotated file name above
	if err != nil {
		return
	}
	zw := gzip.NewWriter(out)
	_, err = io.Copy(zw, in)
	if cerr := zw.Close(); err == nil {
		err = cerr
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, name+".gz")
	}
	if err != nil {
		_ = os.Remove(tmp)
		return
	}
	_ = in.Close()
	_ = os.Remove(name)
}
