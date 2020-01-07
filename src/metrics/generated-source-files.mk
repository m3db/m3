SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
include $(SELF_DIR)/../../.ci/common.mk

gopath_prefix           := $(GOPATH)/src
m3metrics_package       := github.com/m3db/m3/src/metrics
m3metrics_package_path  := $(gopath_prefix)/$(m3metrics_package)
m3x_package             := github.com/m3db/m3/src/x
m3x_package_path        := $(gopath_prefix)/$(m3x_package)

# Generation rule for all generated types
.PHONY: genny-all
genny-all: genny-map-all

# Map generation rule for all generated maps
.PHONY: genny-map-all
genny-map-all: genny-map-matcher-cache-elem genny-map-matcher-namespace-results genny-map-matcher-namespace-rule-sets genny-map-matcher-rule-namespaces

# Map generation rule for matcher/cache/elemMap
.PHONY: genny-map-matcher-cache-elem
genny-map-matcher-cache-elem:
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
genny-map-matcher-namespace-results:
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
genny-map-matcher-namespace-rule-sets:
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
genny-map-matcher-rule-namespaces:
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

