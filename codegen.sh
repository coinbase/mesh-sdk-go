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

OS="$(uname)"
case "${OS}" in
    'Linux')
        OS='linux'
        SED_IFLAG=(-i'')
        ;;
    'Darwin')
        OS='macos'
        SED_IFLAG=(-i '')
        ;;
    *)
        echo "Operating system '${OS}' not supported."
        exit 1
        ;;
esac

# Remove existing clienterated code
rm -rf types;
rm -rf client;
rm -rf server;

# Generate client + types code
docker run --user "$(id -u):$(id -g)" --rm -v "${PWD}":/local openapitools/openapi-generator-cli generate \
  -i /local/spec.json \
  -g go \
  -t /local/templates/client \
  --additional-properties packageName=client\
  -o /local/client;

# Remove unnecessary client files
rm client/go.mod;
rm client/README.md;
rm client/go.mod;
rm client/go.sum;
rm -rf client/api;
rm -rf client/docs;
rm client/git_push.sh;
rm client/.travis.yml;
rm client/.gitignore;
rm client/.openapi-generator-ignore;
rm -rf client/.openapi-generator;

# Add server code
docker run --user "$(id -u):$(id -g)" --rm -v "${PWD}":/local openapitools/openapi-generator-cli generate \
  -i /local/spec.json \
  -g go-server \
  -t /local/templates/server \
  --additional-properties packageName=server\
  -o /local/server;

# Remove unnecessary server files
rm -rf server/api;
rm -rf server/.openapi-generator;
rm server/.openapi-generator-ignore;
rm server/go.mod;
rm server/main.go;
rm server/README.md;
rm server/Dockerfile;
mv server/go/* server/.;
rm -rf server/go;
rm server/model_*.go
rm server/*_service.go


# Fix linting issues
sed "${SED_IFLAG[@]}" 's/Api/API/g' client/* server/*;
sed "${SED_IFLAG[@]}" 's/Json/JSON/g' client/* server/*;
sed "${SED_IFLAG[@]}" 's/Id /ID /g' client/* server/*;
sed "${SED_IFLAG[@]}" 's/Url/URL/g' client/* server/*;

# Remove special characters
sed "${SED_IFLAG[@]}" 's/&#x60;//g' client/* server/*;
sed "${SED_IFLAG[@]}" 's/\&quot;//g' client/* server/*;
sed "${SED_IFLAG[@]}" 's/\&lt;b&gt;//g' client/* server/*;
sed "${SED_IFLAG[@]}" 's/\&lt;\/b&gt;//g' client/* server/*;
sed "${SED_IFLAG[@]}" 's/<code>//g' client/* server/*;
sed "${SED_IFLAG[@]}" 's/<\/code>//g' client/* server/*;

# Fix slice containing pointers
sed "${SED_IFLAG[@]}" 's/\*\[\]/\[\]\*/g' client/* server/*;

# Fix misspellings
sed "${SED_IFLAG[@]}" 's/occured/occurred/g' client/* server/*;
sed "${SED_IFLAG[@]}" 's/cannonical/canonical/g' client/* server/*;
sed "${SED_IFLAG[@]}" 's/Cannonical/Canonical/g' client/* server/*;


# Move model files to types/
mkdir types;
mv client/model_*.go types/;
for file in types/model_*.go; do
    mv "$file" "${file/model_/}"
done

# Change model files to correct package
sed "${SED_IFLAG[@]}" 's/package client/package types/g' types/*;

# Format clienterated code
gofmt -w types/;
gofmt -w client/;
gofmt -w server/;

# Copy in READMEs
cp templates/docs/types.md types/README.md;
cp templates/docs/client.md client/README.md;
cp templates/docs/server.md server/README.md;

# Ensure license correct
make add-license;

# Ensure no long lines
make shorten-lines;
