#!/usr/bin/env bash
set -e
HUGO_DOCKER_IMAGE="klakegg/hugo:ext-alpine"
ASSET_DIR=${1:-src/query/generated/assets/openapi}

# First generate old versions
DOCS_VERSIONS=$(git tag -l 'docs/*')

# Now generate latest
# Copy over OpenAPI doc.
mkdir -p site/static/openapi
rsync -a --exclude=*.go "$ASSET_DIR"/* site/static/openapi
# Create .bak file and then delete it to make sed work for both GNU and Mac versions
sed -i.bak "s#spec-url='.*'#spec-url='spec.yml'#g" site/static/openapi/index.html
rm -f site/static/openapi/index.html.bak

# Now run hugo
if [[ -n "${HUGO_DOCKER:-}" ]]; then
        docker run -e HUGO_ENV=production -it -v "$PWD"/site:/src "${HUGO_DOCKER_IMAGE}"
else
        cd site
        hugo -e production -v
        cd ..
fi

for docVersion in $DOCS_VERSIONS
do
        # Add copies of all docs tags
        IFS='/'
        read -ra version <<< "$docVersion"
        echo "Building $docVersion"
        git archive --output "site/${version[1]}.zip" "$docVersion:site/" && unzip -d "site/${version[1]}" "site/${version[1]}.zip" && rm -f "site/${version[1]}.zip"

        # Versioned docs are served under /${version[1]}, so build them with a
        # matching baseURL. Otherwise every generated link (page listings,
        # "Read more", images, CSS) points at the root site instead of the
        # versioned copy. See https://github.com/m3db/m3/issues/4211
        #
        # Tags up to v1.1 worked around the missing baseURL by hardcoding the
        # prefix into the sidebar links. Strip that, or those links end up
        # doubled up (/v1.0/v1.0/docs/...).
        navbar="site/${version[1]}/layouts/partials/navbar.html"
        if [[ -f "${navbar}" ]]; then
                sed -i.bak -E "s#href=\"/${version[1]}(\{\{ \.RelPermalink\}\})#href=\"\1#g" "${navbar}"
                rm -f "${navbar}.bak"
        fi

        # Now run hugo
        if [[ -n "${HUGO_DOCKER:-}" ]]; then
                docker run -e HUGO_ENV=production -e HUGO_DESTINATION="public/${version[1]}" -e HUGO_BASEURL="/${version[1]}" -it -v "$PWD/site/${version[1]}":/src "${HUGO_DOCKER_IMAGE}"
        else
                cd "site/${version[1]}"
                hugo -e production -v -d "../public/${version[1]}" --baseURL "/${version[1]}"
                cd ../../
        fi        
done
