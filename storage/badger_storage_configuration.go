package storage

// BadgerOption is used to overwrite default values in
// BadgerStorage construction. Any Option not provided
// falls back to the default value.
type BadgerOption func(b *BadgerStorage)

// WithMemoryLimit sets BadgerDB to use
// settings that limit memory.
func WithMemoryLimit() BadgerOption {
	return func(b *BadgerStorage) {
		b.limitMemory = true
	}
}

// WithCompressorEntries provides zstd dictionaries
// for given namespaces.
func WithCompressorEntries(entries []*CompressorEntry) BadgerOption {
	return func(b *BadgerStorage) {
		b.compressorEntries = entries
	}
}
