#!/bin/bash
# Copyright 2024 Coinbase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

go install github.com/incu6us/goimports-reviser/v2@latest;

while IFS= read -r -d '' FILE 
do
  goimports-reviser -file-path "${FILE}" -local github.com/coinbase/rosetta-sdk-go/
done < <(find . -type f -name "*.go" -print0)
