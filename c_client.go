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
	"time"
)

type Instance struct {
	Client  recordbase.Client
}

func (t *Instance) Get(tenant, primaryKey string) *GetBuilder {
	return &GetBuilder {
		Instance: t,
		Request:  &recordpb.GetRequest {
			Tenant: tenant,
			PrimaryKey: primaryKey,
		},
	}
}

func (t *Instance) Close() {
	t.Client.Destroy()
}

type InstanceBuilder struct {
	Endpoint string
	AuthToken string
	UseTls bool
	TimeoutMillis int
}

func (t *InstanceBuilder) Token(token string) *InstanceBuilder {
	t.AuthToken = token
	return t
}

func (t *InstanceBuilder) Tls(tls bool) *InstanceBuilder {
	t.UseTls = tls
	return t
}

func (t *InstanceBuilder) Timeout(timeoutMillis int) *InstanceBuilder {
	t.TimeoutMillis = timeoutMillis
	return t
}

func (t *InstanceBuilder) Connect() (*Instance, error) {

	if t.TimeoutMillis > 0 {
		clientDeadline := time.Now().Add(time.Duration(t.TimeoutMillis) * time.Millisecond)
		ctx, cancel := context.WithDeadline(context.Background(), clientDeadline)
		defer cancel()

		return t.doConnect(ctx)
	} else {
		return t.doConnect(context.Background())
	}

}

func (t *InstanceBuilder) doConnect(ctx context.Context) (*Instance, error) {

	var tlsConfig *tls.Config
	if t.UseTls {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: true,
			Rand:               rand.Reader,
		}
	}

	client, err :=  recordbase.NewClient(ctx, t.Endpoint, t.AuthToken, tlsConfig)
	if err != nil {
		return nil, err
	}

	return &Instance {
		Client: client,
	}, nil
}

func New(commaSeparatedEndpoints string) *InstanceBuilder {
	return &InstanceBuilder {
		Endpoint: commaSeparatedEndpoints,
	}
}

type Entry struct {
	Columns  map[string][]byte
}

type GetBuilder struct {
	Instance *Instance
	Request  *recordpb.GetRequest
	TimeoutMillis int
}

func (t *GetBuilder) FileContents(include bool) *GetBuilder {
	t.Request.FileContents = include
	return t
}

func (t *GetBuilder) Timeout(timeoutMillis int) *GetBuilder {
	t.TimeoutMillis = timeoutMillis
	return t
}

func (t *GetBuilder) ToEntry() (*Entry, error) {

	if t.TimeoutMillis > 0 {

		clientDeadline := time.Now().Add(time.Duration(t.TimeoutMillis) * time.Millisecond)
		ctx, cancel := context.WithDeadline(context.Background(), clientDeadline)
		defer cancel()

		return t.doToEntry(ctx)

	} else {
		return t.doToEntry(context.Background())
	}

}

func (t *GetBuilder) doToEntry(ctx context.Context) (*Entry, error) {

	resp, err := t.Instance.Client.Get(ctx, t.Request)
	if err != nil {
		return nil, err
	}

	m := make(map[string][]byte)
	for _, entry := range resp.Columns {
		m[entry.Name] = entry.Value
	}

	return &Entry {
		Columns:  m,
	}, nil

}

func Sum(a, b int) int {
	return a + b
}

