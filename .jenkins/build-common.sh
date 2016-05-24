. "$(dirname $0)/variables.sh"

if [ -z $GOPATH ]; then
  export GOPATH=~/.gocode
  [ -d $GOPATH ] || mkdir $GOPATH
fi

export PATH="$PATH:$GOPATH/bin:/usr/local/go/bin"

git submodule update --recursive

rm -rf src/$PACKAGE
mkdir -p $(dirname src/$PACKAGE)
ln -s $PWD src/$PACKAGE

export GOPATH="$GOPATH:$PWD"
