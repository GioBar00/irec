// Copyright 2023 Anapaya Systems
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package serrors is a stub for the actual serrors package. It allows the
// serrorscheck analyzer tests to run.
package serrors

func New(string, ...any) error            { return nil }
func Wrap(error, error, ...any) error     { return nil }
func WrapStr(error, string, ...any) error { return nil }
func WithCtx(error, ...any) error         { return nil }
