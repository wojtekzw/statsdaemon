#!/bin/bash

set -eo pipefail

dpkg-buildpackage ${CUSTOM_DPKG_BUILD_OPTIONS}

mv -f ../*.deb .
