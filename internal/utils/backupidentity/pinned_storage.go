// Copyright 2023 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backupidentity

import (
	"context"
	"io"

	"github.com/greenmaskio/greenmask/internal/storages"
)

// PinnedStorage wraps a Storager so that every object read is validated
// against the pinned backup generation. Pre-data, data and post-data phases of
// a restore all read through it and can never open an object that disappeared
// or was replaced by another generation.
type PinnedStorage struct {
	storages.Storager
	identity *Identity
}

// NewPinnedStorage wraps st with generation checks against identity.
func NewPinnedStorage(st storages.Storager, identity *Identity) *PinnedStorage {
	return &PinnedStorage{
		Storager: st,
		identity: identity,
	}
}

// GetObject rejects objects that vanished or changed generation before any
// byte is streamed into the target database.
func (p *PinnedStorage) GetObject(ctx context.Context, filePath string) (io.ReadCloser, error) {
	if err := p.identity.VerifyObject(ctx, p.Storager, filePath); err != nil {
		return nil, err
	}
	return p.Storager.GetObject(ctx, filePath)
}

// SubStorage keeps the pin for nested storages. Restore data objects are flat,
// but wrapping children keeps the guarantee if a restorer ever descends.
func (p *PinnedStorage) SubStorage(subPath string, relative bool) storages.Storager {
	return NewPinnedStorage(p.Storager.SubStorage(subPath, relative), p.identity)
}
