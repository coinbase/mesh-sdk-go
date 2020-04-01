#!/bin/bash
# Copyright 2020 Coinbase, Inc.
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


# Remove existing generated code
rm -rf gen;

# Generate new code
docker run --rm -v ${PWD}:/local openapitools/openapi-generator-cli generate \
  -i /local/spec.json \
  -g go \
  -t /local/templates \
  --additional-properties packageName=gen \
  --additional-properties packageVersion=0.0.1 \
  -o /local/gen;

# Remove unnecessary files
mv gen/README.md .;
mv -n gen/go.mod .;
rm gen/go.mod;
rm gen/go.sum;
rm -rf gen/api;
rm -rf gen/docs;
rm gen/git_push.sh;
rm gen/.travis.yml;
rm gen/.gitignore;
rm gen/.openapi-generator-ignore;
rm -rf gen/.openapi-generator;

# Remove special characters
sed -i '' 's/&#x60;//g' gen/*;
sed -i '' 's/\&quot;//g' gen/*;
sed -i '' 's/\&lt;b&gt;//g' gen/*;
sed -i '' 's/\&lt;\/b&gt;//g' gen/*;
sed -i '' 's/<code>//g' gen/*;
sed -i '' 's/<\/code>//g' gen/*;

# Fix slice containing pointers
sed -i '' 's/\*\[\]/\[\]\*/g' gen/*;

# Format generated code
gofmt -w gen/;
