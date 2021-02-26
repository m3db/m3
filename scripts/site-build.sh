#!/usr/bin/env bash
set -e
HUGO_DOCKER_IMAGE="klakegg/hugo:ext-alpine"
ASSET_DIR=${1:-src/query/generated/assets/openapi}

# First generate old versions
docsVersions=$(git tag -l 'docs/*')
for docVersion in $docsVersions
do
        echo "Building $docVersion"
        git checkout -q "tags/$docVersion"
        # Copy over OpenAPI doc.
        mkdir -p site/static/openapi
        rsync -a --exclude=*.go "$ASSET_DIR"/* site/static/openapi
        # Create .bak file and then delete it to make sed work for both GNU and Mac versions
        sed -i.bak "s#spec-url='.*'#spec-url='spec.yml'#g" site/static/openapi/index.html
        rm -f site/static/openapi/index.html.bak
        # Now run hugo
        if [[ -n "${HUGO_DOCKER:-}" ]]; then
                docker run --rm -it -v "$PWD"/site:/src "${HUGO_DOCKER_IMAGE}"
        else
                cd site
                hugo -d "public/${docVersion//docs\/}"
        fi    
        cd ..    
done

# Now generate latest
# Copy over OpenAPI doc.
mkdir -p site/static/openapi
rsync -a --exclude=*.go "$ASSET_DIR"/* site/static/openapi
# Create .bak file and then delete it to make sed work for both GNU and Mac versions
sed -i.bak "s#spec-url='.*'#spec-url='spec.yml'#g" site/static/openapi/index.html
rm -f site/static/openapi/index.html.bak
# Now run hugo
if [[ -n "${HUGO_DOCKER:-}" ]]; then
        docker run --rm -it -v "$PWD"/site:/src "${HUGO_DOCKER_IMAGE}"
else
        cd site
        hugo
fi