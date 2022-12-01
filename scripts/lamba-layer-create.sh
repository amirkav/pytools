#!/usr/bin/env bash
set -e

ROOT_PATH=`P="$0" python -c "from pathlib import Path; import os; print(Path(os.getenv('P')).absolute().parents[1])"`
cd ${ROOT_PATH}

VERSION=`date +'%Y%m%d'`
RUNTIME=${1:-python3.6}
OUTPUT=${ROOT_PATH}/output
LAYER_NAME="altitude-${ENV}-tools-binaries"

echo "Building ${LAYER_NAME} ${VERSION} for ${RUNTIME}"

sudo chown -R $(id -u):$(id -g) $OUTPUT || true
rm -rf ${OUTPUT}
mkdir ${OUTPUT}

# This script creates AWS tools lambda layer

sed "s/python3.6/${RUNTIME}/" scripts/Dockerfile.lambci > scripts/Dockerfile.lambci.temp
docker build . \
    -f scripts/Dockerfile.lambci.temp \
    --build-arg INTERNAL_PYPI_USERNAME=${INTERNAL_PYPI_USERNAME} \
    --build-arg INTERNAL_PYPI_PASS=${INTERNAL_PYPI_PASS} \
    --build-arg INTERNAL_PYPI_URL=${INTERNAL_PYPI_URL} \
    -t lambda-builder

rm scripts/Dockerfile.lambci.temp

docker run \
    -v $OUTPUT:/out \
    --rm -i lambda-builder

sudo chown -R $(id -u):$(id -g) $OUTPUT || true
cd ${OUTPUT}
rm ${ROOT_PATH}/tools-binaries.zip || true
zip -q -r ${ROOT_PATH}/tools-binaries.zip *
cd -
rm -rf ${OUTPUT}

awsv2 lambda publish-layer-version \
    --layer-name ${LAYER_NAME} \
    --description ${VERSION} \
    --zip-file fileb://${ROOT_PATH}/tools-binaries.zip \
    --compatible-runtimes ${RUNTIME}

rm -f ${ROOT_PATH}/tools-binaries.zip
