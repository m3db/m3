m3x_package            := github.com/m3db/m3x
m3x_package_path       := $(gopath_prefix)/$(m3x_package)
m3x_package_min_ver    := 29bc232d9ad2e6c4a1804ccce095bb730fb1c6bc
m3metrics_package_path := $(gopath_prefix)/$(m3metrics_package)

.PHONY: install-m3x-repo
install-m3x-repo: install-glide install-generics-bin
	# Check if repository exists, if not get it
	test -d $(m3x_package_path) || go get -u $(m3x_package)
	test -d $(m3x_package_path)/vendor || (cd $(m3x_package_path) && glide install)
	test "$(shell cd $(m3x_package_path) && git diff --shortstat 2>/dev/null)" = "" || ( \
		echo "WARNING: m3x repository is dirty, generated files might not be as expected" \
	)
	# If does exist but not at min version then update it
	(cd $(m3x_package_path) && git cat-file -t $(m3x_package_min_ver) > /dev/null) || ( \
		echo "WARNING: m3x repository is below commit $(m3x_package_min_ver), generated files might not be as expected" \
	)

# Generation rule for all generated types
.PHONY: genny-all
genny-all: genny-map-all

# Tests that all currently generated types match their contents if they were regenerated
.PHONY: test-genny-all
test-genny-all: test-genny-map-all

# Map generation rule for all generated maps
.PHONY: genny-map-all
genny-map-all: genny-map-matcher-cache-elem genny-map-matcher-namespace-results genny-map-matcher-namespace-rule-sets genny-map-matcher-rule-namespaces

# Tests that all currently generated maps match their contents if they were regenerated
.PHONY: test-genny-map-all
test-genny-map-all: genny-map-all
	@test "$(shell git diff --shortstat 2>/dev/null)" = "" || (git diff --no-color && echo "Check git status, there are dirty files" && exit 1)
	@test "$(shell git status --porcelain 2>/dev/null | grep "^??")" = "" || (git status --porcelain && echo "Check git status, there are untracked files" && exit 1)

# Map generation rule for matcher/cache/elemMap
.PHONY: genny-map-matcher-cache-elem
genny-map-matcher-cache-elem: install-m3x-repo
	cd $(m3x_package_path) && make byteshashmap-gen \
		pkg=cache \
		value_type=elementPtr \
		target_package=$(m3metrics_package)/matcher/cache \
		rename_type_prefix=elem \
		rename_constructor=newElemMap \
		rename_constructor_options=elemMapOptions
	# Rename both generated map and constructor files
	mv -f $(m3metrics_package_path)/matcher/cache/map_gen.go $(m3metrics_package_path)/matcher/cache/elem_map_gen.go
	mv -f $(m3metrics_package_path)/matcher/cache/new_map_gen.go $(m3metrics_package_path)/matcher/cache/elem_new_map_gen.go

# Map generation rule for matcher/cache/namespaceResultsMap
.PHONY: genny-map-matcher-namespace-results
genny-map-matcher-namespace-results: install-m3x-repo
	cd $(m3x_package_path) && make byteshashmap-gen \
		pkg=cache \
		value_type=results \
		target_package=$(m3metrics_package)/matcher/cache \
		rename_type_prefix=namespaceResults \
		rename_constructor=newNamespaceResultsMap \
		rename_constructor_options=namespaceResultsMapOptions
	# Rename both generated map and constructor files
	mv -f $(m3metrics_package_path)/matcher/cache/map_gen.go $(m3metrics_package_path)/matcher/cache/namespace_results_map_gen.go
	mv -f $(m3metrics_package_path)/matcher/cache/new_map_gen.go $(m3metrics_package_path)/matcher/cache/namespace_results_new_map_gen.go

# Map generation rule for matcher/namespaceRuleSetsMap
.PHONY: genny-map-matcher-namespace-rule-sets
genny-map-matcher-namespace-rule-sets: install-m3x-repo
	cd $(m3x_package_path) && make byteshashmap-gen \
		pkg=matcher \
		value_type=RuleSet \
		target_package=$(m3metrics_package)/matcher \
		rename_type_prefix=namespaceRuleSets \
		rename_constructor=newNamespaceRuleSetsMap \
		rename_constructor_options=namespaceRuleSetsMapOptions
	# Rename both generated map and constructor files
	mv -f $(m3metrics_package_path)/matcher/map_gen.go $(m3metrics_package_path)/matcher/namespace_rule_sets_map_gen.go
	mv -f $(m3metrics_package_path)/matcher/new_map_gen.go $(m3metrics_package_path)/matcher/namespace_rule_sets_new_map_gen.go

# Map generation rule for matcher/ruleNamespacesMap
.PHONY: genny-map-matcher-rule-namespaces
genny-map-matcher-rule-namespaces: install-m3x-repo
	cd $(m3x_package_path) && make byteshashmap-gen \
		pkg=matcher \
		value_type=rulesNamespace \
		target_package=$(m3metrics_package)/matcher \
		rename_type_prefix=ruleNamespaces \
		rename_constructor=newRuleNamespacesMap \
		rename_constructor_options=ruleNamespacesMapOptions
	# Rename both generated map and constructor files
	mv -f $(m3metrics_package_path)/matcher/map_gen.go $(m3metrics_package_path)/matcher/rule_namespaces_map_gen.go
	mv -f $(m3metrics_package_path)/matcher/new_map_gen.go $(m3metrics_package_path)/matcher/rule_namespaces_new_map_gen.go

