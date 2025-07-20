package cursor

// MultiCursor struktura
type MultiCursor struct {
	cursors []Cursor
	current Cursor
	minKey  string
	maxKey  string
}

// NewMultiCursor vraca instancu multicursora koja sluzi kao wrapper za sve cursore
func NewMultiCursor(minKey, maxKey string, cursors ...Cursor) *MultiCursor {
	mc := &MultiCursor{
		cursors: cursors,
		minKey:  minKey,
		maxKey:  maxKey,
	}

	// Za svaku cursor pronadji prvi kljuc koji je >= minKey
	for _, c := range mc.cursors {
		c.Seek(minKey)
	}

	return mc
}

// Predji na sledeci zapis
func (mc *MultiCursor) Next() bool {
	var nextCursor Cursor
	var nextKey string

	// Trenutni cursor prebaci na sledeci zapis
	if mc.current != nil {
		mc.current.Next()
	}

	// Pronadji cursor sa najmanjim kljucem u opsegu
	for _, c := range mc.cursors {
		currentKey := c.Key()

		// Preskoci prazne kljuceve i kljuceve izvan opsega
		if currentKey == "" || (mc.maxKey != "" && currentKey > mc.maxKey) {
			continue
		}

		// Izaberi cursor sa najmanjim kljucem
		if nextCursor == nil || currentKey < nextKey {
			nextCursor = c
			nextKey = currentKey
		}
	}

	mc.current = nextCursor

	return mc.current != nil
}

// Getter za kljuc
func (mc *MultiCursor) Key() string {
	if mc.current == nil {
		return ""
	}
	return mc.current.Key()
}

// Getter za vrijednost
func (mc *MultiCursor) Value() []byte {
	if mc.current == nil {
		return nil
	}
	return mc.current.Value()
}

// Getter za timestamp
func (mc *MultiCursor) Timestamp() [16]byte {
	if mc.current == nil {
		return [16]byte{}
	}
	return mc.current.Timestamp()
}

// Getter za tombstone
func (mc *MultiCursor) Tombstone() bool {
	if mc.current == nil {
		return false
	}
	return mc.current.Tombstone()
}

// Funckija za reset cursora
func (mc *MultiCursor) Close() {
	// Pozovi close za svaki cursor
	for _, c := range mc.cursors {
		c.Close()
	}
}
