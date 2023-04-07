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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os"
	"strings"
	"time"
)

var (
	EnvPrefix = "$"
	TLSPrefix = "tls://"
)

type FileInfo struct {
	Name      string `json:"name,omitempty"` // could be a path
	Data      []byte `json:"data,omitempty"`
	Size      int32  `json:"size,omitempty"`
	CreatedAt int64  `json:"created_at,omitempty"`
	UpdatedAt int64  `json:"updated_at,omitempty"`
	DeletedAt int64  `json:"deleted_at,omitempty"`
}

type Entry struct {
	Tenant     string  `json:"tenant,omitempty"`
	PrimaryKey string  `json:"primary_key,omitempty"`
	Version    int64   `json:"version,omitempty"`
	CreatedAt  int64   `json:"created_at,omitempty"`
	UpdatedAt  int64   `json:"updated_at,omitempty"`
	DeletedAt  int64   `json:"deleted_at,omitempty"`
	Attributes map[string]string   `json:"attributes,omitempty"`
	Tags       []string            `json:"tags,omitempty"`
	Columns    map[string][]byte   `json:"columns,omitempty"`
	Files      map[string]FileInfo `json:"files,omitempty"`
}

type Instance struct {
	client  recordbase.Client
}

func (t *Instance) Close() {
	t.client.Destroy()
}

func Connect(endpoint, token string, timeoutMillis int) (*Instance, error) {

	var tls bool
	if strings.HasPrefix(endpoint, TLSPrefix) {
		tls = true
		endpoint = endpoint[len(TLSPrefix):]
	}

	if strings.HasPrefix(token, EnvPrefix) {
		token = os.Getenv(token[len(EnvPrefix):])
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
		client: client,
	}, nil
}

func (t *Instance) Get(tenant, primaryKey string, timeoutMillis int) (*Entry, error) {

	req := &recordpb.GetRequest {
		Tenant:       tenant,
		PrimaryKey:   primaryKey,
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

	resp, err := t.client.Get(ctx, req)
	if err != nil {
		if s, ok := status.FromError(err); ok && s.Code() == codes.NotFound {
			return new(Entry), nil
		}
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

	files := make(map[string]FileInfo)
	for _, entry := range resp.Files {
		files[entry.Name] = FileInfo{
			Name:      entry.Name,
			Size:      entry.Size,
			CreatedAt: entry.CreatedAt,
			UpdatedAt: entry.UpdatedAt,
			DeletedAt: entry.DeletedAt,
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

func (t *Instance) Merge(entry *Entry, timeoutMillis int) error {
	return t.doUpdate(entry, recordpb.UpdateType_MERGE, timeoutMillis)
}

func (t *Instance) Replace(entry *Entry, timeoutMillis int) error {
	return t.doUpdate(entry, recordpb.UpdateType_MERGE, timeoutMillis)
}


func (t *Instance) doUpdate(entry *Entry, updateType recordpb.UpdateType, timeoutMillis int) error {

	req := &recordpb.UpdateRequest {
		Tenant:       entry.Tenant,
		PrimaryKey:   entry.PrimaryKey,
		UpdateType:   updateType,
		Tags:         entry.Tags,
	}

	for name, value := range entry.Attributes {
		req.Attributes = append(req.Attributes, &recordpb.AttributeEntry{
			Name:  name,
			Value: value,
		})
	}

	for name, value := range entry.Columns {
		req.Columns = append(req.Columns, &recordpb.ColumnEntry{
			Name:  name,
			Value: value,
		})
	}

	if timeoutMillis > 0 {

		clientDeadline := time.Now().Add(time.Duration(timeoutMillis) * time.Millisecond)
		ctx, cancel := context.WithDeadline(context.Background(), clientDeadline)
		defer cancel()

		return t.client.Update(ctx, req)

	} else {
		return t.client.Update(context.Background(), req)
	}

}

