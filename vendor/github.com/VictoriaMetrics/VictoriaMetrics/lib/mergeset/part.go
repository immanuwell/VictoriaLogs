package mergeset

import (
	"fmt"
	"path/filepath"
	"sync"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/blockcache"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs/fsutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/objectstorage/common"
)

var idxbCache = blockcache.NewCache(getMaxIndexBlocksCacheSize)
var ibCache = blockcache.NewCache(getMaxInmemoryBlocksCacheSize)
var ibSparseCache = blockcache.NewCache(getMaxInmemoryBlocksSparseCacheSize)

// SetIndexBlocksCacheSize overrides the default size of indexdb/indexBlocks cache
func SetIndexBlocksCacheSize(size int) {
	maxIndexBlockCacheSize = size
}

func getMaxIndexBlocksCacheSize() int {
	maxIndexBlockCacheSizeOnce.Do(func() {
		if maxIndexBlockCacheSize <= 0 {
			maxIndexBlockCacheSize = int(0.10 * float64(memory.Allowed()))
		}
	})
	return maxIndexBlockCacheSize
}

var (
	maxIndexBlockCacheSize     int
	maxIndexBlockCacheSizeOnce sync.Once
)

// SetDataBlocksCacheSize overrides the default size of indexdb/dataBlocks cache
func SetDataBlocksCacheSize(size int) {
	maxInmemoryBlockCacheSize = size
}

func getMaxInmemoryBlocksCacheSize() int {
	maxInmemoryBlockCacheSizeOnce.Do(func() {
		if maxInmemoryBlockCacheSize <= 0 {
			maxInmemoryBlockCacheSize = int(0.25 * float64(memory.Allowed()))
		}
	})
	return maxInmemoryBlockCacheSize
}

// SetDataBlocksSparseCacheSize overrides the default size of indexdb/dataBlocksSparse cache
func SetDataBlocksSparseCacheSize(size int) {
	maxInmemorySparseMergeCacheSize = size
}

func getMaxInmemoryBlocksSparseCacheSize() int {
	maxInmemoryBlockSparseCacheSizeOnce.Do(func() {
		if maxInmemorySparseMergeCacheSize <= 0 {
			maxInmemorySparseMergeCacheSize = int(0.05 * float64(memory.Allowed()))
		}
	})
	return maxInmemorySparseMergeCacheSize
}

var (
	maxInmemoryBlockCacheSize     int
	maxInmemoryBlockCacheSizeOnce sync.Once

	maxInmemorySparseMergeCacheSize     int
	maxInmemoryBlockSparseCacheSizeOnce sync.Once
)

type part struct {
	ph partHeader

	path string

	size uint64

	mrs []metaindexRow

	indexFile fs.ReadAtCloser
	itemsFile fs.ReadAtCloser
	lensFile  fs.ReadAtCloser
}

func openRemotePart(sc common.StorageClient, allPartFiles map[string]uint64, path, name string) (*part, error) {
	var size uint64

	getReader := func(p string) *common.ReadCloser {
		lookupPath := filepath.Join(name, p)
		openPath := filepath.Join(path, name, p)
		if fileSize, ok := allPartFiles[lookupPath]; ok {
			size += fileSize
			return common.NewReadCloser(sc, openPath, fileSize)
		}
		logger.Panicf("FATAL: cannot locate part file %s", sc.GetPath(openPath))
		return nil
	}

	metaindexFile := getReader(metaindexFilename)
	indexFile := getReader(indexFilename)
	itemsFile := getReader(itemsFilename)
	lensFile := getReader(lensFilename)

	metadataLookupPath := filepath.Join(name, metadataFilename)
	metadataOpenPath := filepath.Join(path, name, metadataFilename)
	metadataSize, ok := allPartFiles[metadataLookupPath]
	if !ok {
		logger.Panicf("FATAL: cannot locate part header file %s", sc.GetPath(metadataOpenPath))
	}

	bb := common.GetWriteAtBuffer()
	defer common.PutWriteAtBuffer(bb)
	bb.Grow(int(metadataSize))
	bb.B = bb.B[:int(metadataSize)]
	if err := sc.ReadFile(metadataOpenPath, bb); err != nil {
		return nil, fmt.Errorf("cannot get header file %s: %w", sc.GetPath(metadataOpenPath), err)
	}

	var ph partHeader
	if err := ph.readMetadata(bb.B); err != nil {
		return nil, err
	}
	return newPart(&ph, path, size, metaindexFile, indexFile, itemsFile, lensFile)
}

func mustOpenFilePart(path string) *part {
	var ph partHeader
	ph.mustReadLocalMetadata(path)

	metaindexPath := filepath.Join(path, metaindexFilename)
	metaindexFile := filestream.MustOpen(metaindexPath, true)
	metaindexSize := fs.MustFileSize(metaindexPath)

	// Open part files in parallel in order to speed up this process
	// on high-latency storage systems such as NFS or Ceph.

	var pe fsutil.ParallelExecutor

	indexPath := filepath.Join(path, indexFilename)
	itemsPath := filepath.Join(path, itemsFilename)
	lensPath := filepath.Join(path, lensFilename)

	var indexFile fs.ReadAtCloser
	var indexSize uint64
	pe.Add(fs.NewReaderAtOpenerTask(indexPath, &indexFile, &indexSize))

	var itemsFile fs.ReadAtCloser
	var itemsSize uint64
	pe.Add(fs.NewReaderAtOpenerTask(itemsPath, &itemsFile, &itemsSize))

	var lensFile fs.ReadAtCloser
	var lensSize uint64
	pe.Add(fs.NewReaderAtOpenerTask(lensPath, &lensFile, &lensSize))

	pe.Run()

	size := metaindexSize + indexSize + itemsSize + lensSize
	p, err := newPart(&ph, path, size, metaindexFile, indexFile, itemsFile, lensFile)
	if err != nil {
		logger.Panicf("FATAL: %s", err)
	}
	return p
}

func newPart(ph *partHeader, path string, size uint64, metaindexReader filestream.ReadCloser, indexFile, itemsFile, lensFile fs.ReadAtCloser) (*part, error) {
	mrs, err := unmarshalMetaindexRows(nil, metaindexReader)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal metaindexRows from %s: %w", path, err)
	}
	metaindexReader.MustClose()

	var p part
	p.path = path
	p.size = size
	p.mrs = mrs

	p.indexFile = indexFile
	p.itemsFile = itemsFile
	p.lensFile = lensFile

	p.ph.CopyFrom(ph)
	return &p, nil
}

func (p *part) MustClose() {
	// Close files in parallel in order to speed up this process on storage systems with high latency
	// such as NFS or Ceph.
	var pe fsutil.ParallelExecutor
	pe.Add(fs.NewCloserTask(p.indexFile))
	pe.Add(fs.NewCloserTask(p.itemsFile))
	pe.Add(fs.NewCloserTask(p.lensFile))
	pe.Run()

	idxbCache.RemoveBlocksForPart(p)
	ibCache.RemoveBlocksForPart(p)
	ibSparseCache.RemoveBlocksForPart(p)
}

type indexBlock struct {
	bhs []blockHeader

	// The buffer for holding the data referred by bhs
	buf []byte
}

func (idxb *indexBlock) SizeBytes() int {
	bhs := idxb.bhs[:cap(idxb.bhs)]
	n := int(unsafe.Sizeof(*idxb))
	for i := range bhs {
		n += bhs[i].SizeBytes()
	}
	return n
}
