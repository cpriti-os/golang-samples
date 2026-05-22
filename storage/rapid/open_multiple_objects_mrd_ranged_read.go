// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rapid

// [START storage_open_multiple_objects_mrd_ranged_read]
import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"cloud.google.com/go/storage/experimental"
)

// openMultipleObjectsMRDRangedRead performs multiple ranged reads on multiple objects
// concurrently by instantiating a MultiRangeDownloader per object.
func openMultipleObjectsMRDRangedRead(w io.Writer, bucket string, objects []string) ([][]byte, error) {
	// bucket := "bucket-name"
	// objects := []string{"object-name1", "object-name2"}
	ctx := context.Background()
	client, err := storage.NewGRPCClient(ctx, experimental.WithZonalBucketAPIs())
	if err != nil {
		return nil, fmt.Errorf("storage.NewGRPCClient: %w", err)
	}
	defer client.Close()

	// Timeout for all concurrent downloads
	ctx, cancel := context.WithTimeout(ctx, time.Second*60)
	defer cancel()

	var wg sync.WaitGroup
	byteSlices := make([][]byte, len(objects))
	errChan := make(chan error, len(objects))

	for i, obj := range objects {
		wg.Add(1)
		go func(idx int, objectName string) {
			defer wg.Done()

			// Create a MultiRangeDownloader specifically for this object
			mrd, err := client.Bucket(bucket).Object(objectName).NewMultiRangeDownloader(ctx)
			if err != nil {
				errChan <- fmt.Errorf("NewMultiRangeDownloader for %s: %w", objectName, err)
				return
			}

			var buf bytes.Buffer
			var mrdErr error

			// Add multiple non-contiguous range downloads for this object
			// Download range 1: 0 to 1024 bytes
			mrd.Add(&buf, 0, 1024, func(off, length int64, err error) {
				if err != nil {
					mrdErr = err
				}
			})
			// Download range 2: 2048 to 3072 bytes (1024 bytes length)
			mrd.Add(&buf, 2048, 1024, func(off, length int64, err error) {
				if err != nil {
					mrdErr = err
				}
			})

			// Wait for download to complete
			mrd.Wait()
			if mrdErr != nil {
				errChan <- fmt.Errorf("MultiRangeDownloader error for %s: %w", objectName, mrdErr)
				return
			}
			if err := mrd.Close(); err != nil {
				errChan <- fmt.Errorf("MultiRangeDownloader close error for %s: %w", objectName, err)
				return
			}

			fmt.Fprintf(w, "Downloaded multiple ranges of %v from bucket %v\n", objectName, bucket)
			byteSlices[idx] = buf.Bytes()
		}(i, obj)
	}

	wg.Wait()
	close(errChan)

	if len(errChan) > 0 {
		return nil, <-errChan
	}

	return byteSlices, nil
}

// [END storage_open_multiple_objects_mrd_ranged_read]
