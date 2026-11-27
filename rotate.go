package slogx

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	// the owner's group (such as log monitoring services) and no
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
// .slogx-archives/<full filename>/20261108T031245.000[-N][.gz].
const rotatedLayout = "20060102T150405.000"
const archiveRoot = ".slogx-archives"

// Rotating is an Output to a file that rotates itself: the full file is
// moved into .slogx-archives/<full filename> under its UTC timestamp.
// A new active file is then opened. A background worker compresses and
// prunes archives while the writer continues to append and rotate.
type Rotating struct {
	file   atomic.Pointer[rotatingFile] // read by matchers to identify shared destinations
	path   string                       // absolute configured path, including symlinks, for Reopen
	lease  sync.RWMutex
	result error
}

var activeFiles = struct {
	sync.Mutex
	files map[string]*rotatingFile
}{files: make(map[string]*rotatingFile)}
var errRotationConflict = errors.New("slogx: conflicting rotation for active file")

type rotatingFile struct {
	archiveMu   sync.Mutex    // pairs archive name selection with plain-to-gzip publication
	done        chan struct{} // closed after the last handle finishes maintenance
	refs        int           // guarded by activeFiles
	deadline    time.Time
	maintenance error
	f           *os.File
	identity    atomic.Pointer[os.FileInfo] // identity of the opened file, even after an external rename
	work        chan struct{}               // a rotation happened: compress and prune
	timer       *time.Timer                 // writes buf out FlushInterval after its first line
	buf         []byte                      // lines not yet written, with BufferSize
	late        error                       // a timed flush failed; the next call returns it
	dir         string
	cfg         Rotation
	opened      time.Time
	size        int64
	workers     sync.WaitGroup
	start       sync.Once
	mu          sync.Mutex
	closed      bool
}

const defaultFlushInterval = time.Second

// OpenRotating opens the file of c, creating its directory. Compatible opens
// share rotation and maintenance. Close each returned handle. A conflicting
// rotation policy or hard-link alias of an active file returns an error.
func OpenRotating(c Rotation) (*Rotating, error) {
	r, err := reserveRotating(c)
	if err == nil {
		r.file.Load().activate(compressFile)
	}
	return r, err
}

// A compressor prepares a complete temporary file beside the plain archive.
type compressor func(string, os.FileMode) (string, error)

func (r *rotatingFile) activate(compress compressor) {
	r.start.Do(func() {
		r.workers.Go(func() { r.maintain(compress) })
		r.work <- struct{}{}
	})
}

func reserveRotating(c Rotation) (*Rotating, error) {
	c.Mode = c.mode()
	c.BufferSize = max(c.BufferSize, 0)
	switch {
	case c.BufferSize == 0:
		c.FlushInterval = 0
	case c.FlushInterval == 0:
		c.FlushInterval = defaultFlushInterval
	case c.FlushInterval < 0:
		c.FlushInterval = -1
	}
	c.MaxAge = max(c.MaxAge, 0)
	c.MaxFiles = max(c.MaxFiles, 0)
	c.Every = max(c.Every, 0)
	c.MaxSize = max(c.MaxSize, -1)
	if c.MaxSize == 0 {
		c.MaxSize = defaultMaxSize
	}
	path, err := filepath.Abs(c.Path)
	if err != nil {
		return nil, err
	}
	// The returned Rotating owns the reference and releases it in Close.
	//goland:noinspection GoResourceLeak
	f, err := reserveFile(c, path)
	if err != nil {
		return nil, err
	}
	h := &Rotating{path: path}
	h.file.Store(f)
	return h, nil
}

// reserveFile takes a reference using an already normalized rotation policy.
func reserveFile(c Rotation, path string) (*rotatingFile, error) {
	activeFiles.Lock()
	defer activeFiles.Unlock()
	var err error
retry:
	c.Path, err = rotationTarget(path, nil)
	if err != nil {
		return nil, err
	}
	for part := range strings.SplitSeq(filepath.ToSlash(c.Path), "/") {
		if strings.EqualFold(part, archiveRoot) {
			return nil, fmt.Errorf("%w: reserved archive directory in %s", errRotationConflict, path)
		}
	}
	info, _ := os.Stat(c.Path)
	var current *rotatingFile
	for name, r := range activeFiles.files {
		same := name == c.Path || (runtime.GOOS == "windows" && strings.EqualFold(name, c.Path))
		if same {
			current = r
		} else if info != nil && os.SameFile(info, *r.identity.Load()) {
			return nil, fmt.Errorf("%w: %s", errRotationConflict, path)
		}
	}
	if current != nil {
		c.Path = current.cfg.Path
		if current.refs == 0 {
			activeFiles.Unlock()
			<-current.done
			activeFiles.Lock()
			goto retry
		}
		if c != current.cfg {
			return nil, fmt.Errorf("%w: %s", errRotationConflict, path)
		}
		current.refs++
		return current, nil
	}
	r := &rotatingFile{refs: 1, done: make(chan struct{}), cfg: c, dir: filepath.Join(filepath.Dir(c.Path), archiveRoot, filepath.Base(c.Path)), work: make(chan struct{}, 1)}
	if err = os.MkdirAll(filepath.Dir(c.Path), dirMode(c.Mode)); err != nil {
		return nil, err
	}
	if err := r.open(); err != nil {
		return nil, err
	}
	if c.BufferSize > 0 {
		r.buf = make([]byte, 0, c.BufferSize)
	}
	activeFiles.files[c.Path] = r
	return r, nil
}

func rotationTarget(path string, links map[string]bool) (string, error) {
	target, err := filepath.EvalSymlinks(path)
	if !errors.Is(err, os.ErrNotExist) || filepath.Dir(path) == path {
		return target, err
	}
	// Resolve the existing ancestors before creating any missing components.
	parent, err := rotationTarget(filepath.Dir(path), links)
	if err != nil {
		return "", err
	}
	target = filepath.Join(parent, filepath.Base(path))
	info, err := os.Lstat(target)
	if errors.Is(err, os.ErrNotExist) || (err == nil && info.Mode()&os.ModeSymlink == 0) {
		return target, nil
	}
	if err != nil {
		return "", err
	}
	if links[target] {
		return "", fmt.Errorf("cyclic rotation path %s", target)
	}
	if links == nil {
		links = make(map[string]bool)
	}
	links[target] = true
	link, err := os.Readlink(target)
	if err != nil {
		return "", err
	}
	if !filepath.IsAbs(link) {
		link = filepath.Join(parent, link)
	}
	return rotationTarget(link, links)
}

func (r *rotatingFile) open() error {
	f, err := os.OpenFile(r.cfg.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, r.cfg.Mode)
	if err != nil {
		return err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	r.f, r.size, r.opened = f, fi.Size(), time.Now()
	r.identity.Store(&fi)
	return nil
}

// Write appends p to the file, or to the buffer with BufferSize, rotating
// the file first when p would take it past MaxSize or the file is older
// than Every. With a buffer, the error of writing earlier lines out,
// including by the FlushInterval timer, is returned by the next Write,
// Flush or Close.
func (r *Rotating) Write(p []byte) error {
	r.lease.RLock()
	defer r.lease.RUnlock()
	f := r.file.Load()
	if f == nil {
		return os.ErrClosed
	}
	return f.Write(p)
}

func (r *rotatingFile) Write(p []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return errors.Join(r.takeLate(), r.write(p))
}

// takeLate returns and clears the error of a timed flush; r.mu is held.
func (r *rotatingFile) takeLate() error {
	err := errors.Join(r.late, r.maintenance)
	r.late, r.maintenance = nil, nil
	return err
}

func (r *rotatingFile) write(p []byte) error {
	if r.closed {
		return os.ErrClosed
	}
	if r.f == nil {
		if err := r.open(); err != nil {
			return err
		}
	}
	size := r.size + int64(len(r.buf))
	bySize := r.cfg.MaxSize > 0 && size > 0 && size+int64(len(p)) > r.cfg.MaxSize
	byTime := r.cfg.Every > 0 && size > 0 && time.Since(r.opened) >= r.cfg.Every
	var rotationErr error
	if bySize || byTime {
		if err := r.writeBuffered(); err != nil {
			return err
		}
		rotationErr = r.rotate()
		if r.f == nil {
			return rotationErr
		}
	}
	return errors.Join(rotationErr, r.appendLine(p))
}

func (r *rotatingFile) appendLine(p []byte) error {
	if len(r.buf)+len(p) > cap(r.buf) {
		if err := r.writeBuffered(); err != nil {
			return err
		}
	}
	if len(p) >= cap(r.buf) {
		_, err := r.direct(p)
		return err
	}
	if r.deadline.IsZero() {
		r.armTimer()
	}
	r.buf = append(r.buf, p...)
	return nil
}

func (r *rotatingFile) direct(p []byte) (int, error) {
	if r.f == nil {
		return 0, os.ErrClosed
	}
	n, err := r.f.Write(p)
	r.size += int64(n)
	return n, err
}

// writeBuffered retains the unwritten suffix for the next flush.
func (r *rotatingFile) writeBuffered() error {
	if len(r.buf) == 0 {
		return nil
	}
	if r.timer != nil {
		r.timer.Stop()
	}
	r.deadline = time.Time{}
	n, err := r.direct(r.buf)
	r.buf = r.buf[:copy(r.buf, r.buf[n:])]
	return err
}

// armTimer schedules writing out the buffer that a line is about to start;
// r.mu is held.
func (r *rotatingFile) armTimer() {
	d := r.cfg.FlushInterval
	if d < 0 {
		return
	}
	r.deadline = time.Now().Add(d)
	if r.timer == nil {
		r.timer = time.AfterFunc(d, r.flushOnTime)
		return
	}
	r.timer.Reset(d)
}

// flushOnTime writes out the buffer when FlushInterval has passed. Its
// error has no caller, so it is kept for the next Write, Flush or Close.
func (r *rotatingFile) flushOnTime() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed && !time.Now().Before(r.deadline) {
		r.late = errors.Join(r.late, r.writeBuffered())
	}
}

// rotate renames the current file to a name no other rotated file has and
// starts a new one; r.mu is held and the buffer written out.
func (r *rotatingFile) rotate() error {
	r.archiveMu.Lock()
	defer r.archiveMu.Unlock()
	// External rotation may have moved the open file. Keep writing to the
	// open descriptor until Reopen; the replacement belongs to its creator.
	current, err := os.Lstat(r.cfg.Path)
	if errors.Is(err, os.ErrNotExist) || (err == nil && !os.SameFile(*r.identity.Load(), current)) {
		return nil
	}
	if err != nil {
		return err
	}
	if err = r.archiveDirectory(true); err != nil {
		return err
	}
	err = r.f.Close()
	r.f = nil
	stamp := time.Now().UTC().Format(rotatedLayout)
	var name string
	for i := 0; ; i++ {
		suffix := ""
		if i > 0 {
			suffix = "-" + strconv.Itoa(i)
		}
		name = filepath.Join(r.dir, stamp+suffix)
		available, probeErr := archiveNameAvailable(name)
		if probeErr != nil {
			return errors.Join(err, fmt.Errorf("probe archive: %w", probeErr), r.open())
		}
		if available {
			break
		}
	}
	if err == nil {
		if e := os.Rename(r.cfg.Path, name); e != nil {
			err = fmt.Errorf("publish archive: %w", e)
		}
	}
	if err == nil {
		select {
		case r.work <- struct{}{}:
		default: // a pass is already due; it sees this file too
		}
	}
	return errors.Join(err, r.open())
}

func archiveNameAvailable(name string) (bool, error) {
	for _, candidate := range []string{name, name + ".gz"} {
		_, err := os.Lstat(candidate)
		if err == nil {
			return false, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	return true, nil
}

// maintain prunes and then compresses the rotated files after each
// rotation. Each pass looks at every rotated file, so rotations that come
// faster than a pass are never left behind.
func (r *rotatingFile) maintain(compress compressor) {
	for range r.work {
		files, err := r.scan()
		if err == nil {
			err = r.maintainFiles(files, compress)
		}
		if err != nil {
			r.mu.Lock()
			if r.maintenance == nil {
				r.maintenance = err
			}
			r.mu.Unlock()
		}
	}
}

// Reopen flushes the file and follows the configured path again. If a
// symlink now points elsewhere, only this handle moves to the new target.
func (r *Rotating) Reopen() error {
	r.lease.Lock()
	defer r.lease.Unlock()
	old := r.file.Load()
	if old == nil {
		return os.ErrClosed
	}
	// The handle retains the reference after a successful switch and releases it immediately otherwise.
	//goland:noinspection GoResourceLeak
	target, err := reserveFile(old.cfg, r.path)
	if err != nil {
		return err
	}
	if target == old {
		return errors.Join(target.release(), old.Reopen())
	}
	if err = old.Flush(); err != nil {
		return errors.Join(err, target.release())
	}
	target.activate(compressFile)
	r.file.Store(target)
	return old.release()
}

func (r *rotatingFile) Reopen() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return os.ErrClosed
	}
	if err := r.writeBuffered(); err != nil {
		return err
	}
	previous := r.f
	if err := r.open(); err != nil {
		return err
	}
	if previous != nil {
		return previous.Close()
	}
	return nil
}

// Flush writes out the buffer and syncs the file to disk.
func (r *Rotating) Flush() error {
	r.lease.RLock()
	defer r.lease.RUnlock()
	f := r.file.Load()
	if f == nil {
		return os.ErrClosed
	}
	return f.Flush()
}

func (r *rotatingFile) Flush() error {
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
	r.lease.Lock()
	defer r.lease.Unlock()
	if f := r.file.Swap(nil); f != nil {
		r.result = errors.Join(f.Flush(), f.release())
	}
	return r.result
}

func (r *rotatingFile) release() error {
	activeFiles.Lock()
	r.refs--
	last := r.refs == 0
	activeFiles.Unlock()
	if !last {
		return nil
	}
	err := r.Close()
	activeFiles.Lock()
	delete(activeFiles.files, r.cfg.Path)
	close(r.done)
	activeFiles.Unlock()
	return err
}

func (r *rotatingFile) Close() error {
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
	return errors.Join(err, r.maintenance)
}

// rotated is a file that Rotating made, with the time in its name.
type rotated struct {
	path       string
	at         time.Time
	temporary  bool
	compressed bool
	seq        int
}

func (r *rotatingFile) scan() ([]rotated, error) {
	if err := r.archiveDirectory(false); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		return nil, err
	}
	var out []rotated
	for _, e := range entries {
		n := e.Name()
		if !e.Type().IsRegular() {
			continue
		}
		rest := n
		archive, suffix, hasTemp := strings.Cut(rest, ".gz.tmp")
		temporary := hasTemp && (suffix == "" || validTempSuffix(suffix))
		if temporary {
			rest = archive
		}
		rest, compressed := strings.CutSuffix(rest, ".gz")
		if len(rest) < len(rotatedLayout) {
			continue
		}
		at, err := time.Parse(rotatedLayout, rest[:len(rotatedLayout)])
		if err != nil || at.Format(rotatedLayout) != rest[:len(rotatedLayout)] {
			continue
		}
		seq := 0
		if tail := rest[len(rotatedLayout):]; tail != "" {
			if tail[0] != '-' {
				continue
			}
			if seq, err = strconv.Atoi(tail[1:]); err != nil || seq < 1 || strconv.Itoa(seq) != tail[1:] {
				continue
			}
		}
		out = append(out, rotated{path: filepath.Join(r.dir, n), at: at, seq: seq, compressed: compressed, temporary: temporary})
	}
	slices.SortFunc(out, func(a, b rotated) int {
		if c := b.at.Compare(a.at); c != 0 {
			return c
		}
		return b.seq - a.seq
	})
	return out, nil
}

// Each target owns a directory. A link in either component could make
// retention for one target delete another target's files.
func (r *rotatingFile) archiveDirectory(create bool) error {
	for _, dir := range []string{filepath.Dir(r.dir), r.dir} {
		if create {
			if err := os.Mkdir(dir, dirMode(r.cfg.Mode)); err != nil && !errors.Is(err, os.ErrExist) {
				return err
			}
		}
		info, err := os.Lstat(dir)
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("invalid archive directory %s", dir)
		}
	}
	return nil
}

// maintainFiles counts compressed and plain forms as one archive.
func (r *rotatingFile) maintainFiles(files []rotated, compress compressor) error {
	var errs error
	kept := 0
	previous := ""
	for _, f := range files {
		if f.temporary {
			errs = errors.Join(errs, os.Remove(f.path))
			continue
		}
		key := f.path
		if f.compressed {
			key = strings.TrimSuffix(key, ".gz")
		}
		if previous != key {
			kept++
			previous = key
		}
		if r.cfg.MaxFiles > 0 && kept > r.cfg.MaxFiles || r.cfg.MaxAge > 0 && time.Since(f.at) > r.cfg.MaxAge {
			errs = errors.Join(errs, os.Remove(f.path))
			continue
		}
		if r.cfg.Compress && !f.compressed {
			tmp, err := compress(f.path, r.cfg.Mode)
			if err == nil {
				// The plain name stays occupied throughout compression. Move
				// from plain to gzip atomically with respect to name probes.
				r.archiveMu.Lock()
				err = os.Rename(tmp, f.path+".gz")
				if err == nil {
					err = os.Remove(f.path)
				} else {
					err = errors.Join(err, os.Remove(tmp))
				}
				r.archiveMu.Unlock()
			}
			if err != nil {
				errs = errors.Join(errs, fmt.Errorf("compress archive %s: %w", f.path, err))
			}
		}
	}
	return errs
}

// compressFile returns the completed temporary gzip for publication.
func compressFile(name string, mode os.FileMode) (string, error) {
	in, err := os.Open(name) //nolint:gosec // The archive scan selects regular files with valid rotation names in the log's archive directory.
	if err != nil {
		return "", err
	}
	defer func() { _ = in.Close() }()
	out, err := os.CreateTemp(filepath.Dir(name), filepath.Base(name)+".gz.tmp-*")
	if err != nil {
		return "", err
	}
	tmp := out.Name()
	// CreateTemp starts private. Apply only permission bits permitted on the source archive.
	fi, err := in.Stat()
	if err == nil {
		err = out.Chmod(mode & fi.Mode().Perm())
	}
	if err != nil {
		return "", errors.Join(err, out.Close(), os.Remove(tmp))
	}
	zw := gzip.NewWriter(out)
	_, err = io.Copy(zw, in)
	if cerr := zw.Close(); err == nil {
		err = cerr
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return "", errors.Join(err, os.Remove(tmp))
	}
	return tmp, nil
}

// CreateTemp replaces its trailing star with decimal digits.
func validTempSuffix(s string) bool {
	digits, ok := strings.CutPrefix(s, "-")
	if !ok || digits == "" {
		return false
	}
	for _, c := range digits {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
