/*
 * Copyright (c) 2022-2023 Zander Schwid & Co. LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package pyrecordbase

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"github.com/recordbase/recordbase"
	"github.com/recordbase/recordpb"
	"os"
	"strings"
	"time"
)

type Instance struct {
	Client  recordbase.Client
}

func (t *Instance) Close() {
	t.Client.Destroy()
}

func Connect(endpoint, token string, tls bool, timeoutMillis int) (*Instance, error) {

	if strings.HasPrefix(token, "env:") {
		token = os.Getenv(token[4:])
	}

	if timeoutMillis > 0 {
		clientDeadline := time.Now().Add(time.Duration(timeoutMillis) * time.Millisecond)
		ctx, cancel := context.WithDeadline(context.Background(), clientDeadline)
		defer cancel()

		return doConnect(ctx, endpoint, token, tls)
	} else {
		return doConnect(context.Background(), endpoint, token, tls)
	}

}

func doConnect(ctx context.Context, endpoint, token string, useTLS bool) (*Instance, error) {

	var tlsConfig *tls.Config
	if useTLS {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: true,
			Rand:               rand.Reader,
		}
	}

	client, err :=  recordbase.NewClient(ctx, endpoint, token, tlsConfig)
	if err != nil {
		return nil, err
	}

	return &Instance {
		Client: client,
	}, nil
}

type Entry struct {
	Tenant     string
	PrimaryKey string
	Version    int64
	CreatedAt  int64
	UpdatedAt  int64
	DeletedAt  int64
	Attributes map[string]string
	Tags       []string
	Columns    map[string][]byte
	Files      map[string]*FileEntry
}

type FileEntry struct {
	Name      string
	Data      []byte
	Size      int32
	CreatedAt int64
}

func (t *Instance) Get(tenant, primaryKey string, fileContents bool, timeoutMillis int) (*Entry, error) {

	req := &recordpb.GetRequest {
		Tenant:       tenant,
		PrimaryKey:   primaryKey,
		FileContents: fileContents,
	}

	if timeoutMillis > 0 {

		clientDeadline := time.Now().Add(time.Duration(timeoutMillis) * time.Millisecond)
		ctx, cancel := context.WithDeadline(context.Background(), clientDeadline)
		defer cancel()

		return t.doGet(ctx, req)

	} else {
		return t.doGet(context.Background(), req)
	}

}

func (t *Instance) doGet(ctx context.Context, req *recordpb.GetRequest) (*Entry, error) {

	resp, err := t.Client.Get(ctx, req)
	if err != nil {
		return nil, err
	}

	attrs := make(map[string]string)
	for _, entry := range resp.Attributes {
		attrs[entry.Name] = entry.Value
	}

	columns := make(map[string][]byte)
	for _, entry := range resp.Columns {
		columns[entry.Name] = entry.Value
	}

	files := make(map[string]*FileEntry)
	for _, entry := range resp.Files {
		files[entry.Name] = &FileEntry{
			Name:      entry.Name,
			Data:      entry.Data,
			Size:      entry.Size,
			CreatedAt: entry.CreatedAt,
		}
	}

	return &Entry {
		Tenant: resp.Tenant,
		PrimaryKey: resp.PrimaryKey,
		Version: resp.Version,
		CreatedAt: resp.CreatedAt,
		UpdatedAt: resp.UpdatedAt,
		DeletedAt: resp.DeletedAt,
		Attributes: attrs,
		Tags: resp.Tags,
		Columns:  columns,
		Files: files,
	}, nil

}

func Sum(a, b int) int {
	return a + b
}

