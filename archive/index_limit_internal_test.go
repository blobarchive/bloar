package archive

import (
	"errors"
	"testing"
)

func TestIndexNodeAdmissionUsesSharedReaderBoundary(t *testing.T) {
	if MaxIndexNodeBytes != MaxEnumerationNodeBytes {
		t.Fatalf("writer limit %d differs from reader limit %d", MaxIndexNodeBytes, MaxEnumerationNodeBytes)
	}
	head := &Head{segmentEncodeState: SegmentSealed}
	if err := head.admitEncodedIndex(IndexNodeSegment, MaxIndexNodeBytes, 1, 2); err != nil {
		t.Fatalf("node exactly at the shared boundary was refused: %v", err)
	}

	for _, kind := range []IndexNodeKind{IndexNodeHead, IndexNodeDir, IndexNodeSegment} {
		err := head.admitEncodedIndex(kind, MaxIndexNodeBytes+1, 3, 4)
		var oversized *IndexNodeTooLargeError
		if !errors.As(err, &oversized) {
			t.Fatalf("%s refusal = %v, want *IndexNodeTooLargeError", kind, err)
		}
		if oversized.Kind != kind || oversized.EncodedBytes != MaxIndexNodeBytes+1 ||
			oversized.LimitBytes != MaxIndexNodeBytes {
			t.Errorf("%s refusal = %#v", kind, oversized)
		}
		if kind == IndexNodeSegment {
			if oversized.State != SegmentSealed || oversized.Rows != 3 || oversized.Refs != 4 {
				t.Errorf("Segment refusal lost density context: %#v", oversized)
			}
		}
	}
}
