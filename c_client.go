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

type Instance struct {
	client  recordbase.Client
}

func (t *Instance) Close() {
	t.client.Destroy()
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
		client: client,
	}, nil
}

func (t *Instance) Get(tenant, primaryKey string, timeoutMillis int) (map[string]interface{}, error) {

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

func (t *Instance) doGet(ctx context.Context, req *recordpb.GetRequest) (map[string]interface{}, error) {

	resp, err := t.client.Get(ctx, req)
	if err != nil {
		if s, ok := status.FromError(err); ok && s.Code() == codes.NotFound {
			return make(map[string]interface{}), nil
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

	files := make(map[string]interface{})
	for _, entry := range resp.Files {
		files[entry.Name] = map[string]interface{}{
			"Name":      entry.Name,
			"Size":      entry.Size,
			"CreatedAt": entry.CreatedAt,
		}
	}

	return map[string]interface{} {
		"Tenant": resp.Tenant,
		"PrimaryKey": resp.PrimaryKey,
		"Version": resp.Version,
		"CreatedAt": resp.CreatedAt,
		"UpdatedAt": resp.UpdatedAt,
		"DeletedAt": resp.DeletedAt,
		"Attributes": attrs,
		"Tags": resp.Tags,
		"Columns":  columns,
		"Files": files,
	}, nil

}

func Sum(a, b int) int {
	return a + b
}

