# Tips For Writing Documentation

Writing is easy. Writing _well_ is hard. Writing documentation is even harder and, as one might
expect, writing _good documentation_ can be very hard. The challenge ultimately stems from the fact
that good documentation requires attention to several things: consistency, clarity, detail,
information density, unambiguity, and grammar (of course).

This page describes tips for writing documentation (for M3 or otherwise). There are many different,
equally-valid writing styles; this document isn't meant as a prescription for how to write, but
instead as optional (but encouraged) guidelines geared toward writing consistent, clear,
easy-to-read documentation.

## Tip #1: Avoid first- and second-person pronouns.

Documentation should always be written from a [third-person point of view][wp3p] (specifically,
with a [third-person objective narrative voice][wp3ponv]); avoid using [first-][wp1p] and
[second-person][wp2p]. The point of documentation is not to be a message from the author or to
convey their point of view, but rather to be objective, factual, and canonical.

| Examples | |
| :-: | --- |
| BAD | ~~You will need to set Value X to "foo", as our testing indicated that "bar" can cause issues when solar rays are present.~~ |
| BAD | ~~We suggest setting Value X to "foo", as testing indicated that "bar" can cause issues when solar rays are present.~~ |
| GOOD | **Value X must not be set to "bar", as testing indicated that it may cause issues when solar rays are present.** |
| GOOD | **Value X must be set to "foo"; a value of "bar" may cause issues when solar rays are present.** |

## Tip #2: Avoid subjective language.

Subjective language – language that is open to interpretation, particularly based on perspective –
should be avoided wherever possible. Documentation should state objective or broadly empirical
truths (and should be supported by facts). Introducing perspective inherently creates ambiguity and
vagueness in the document, as its meaning becomes reader-dependent. Importantly, cases of subjective
language are distinct from cases of conditional behavior.

| Examples | |
| :-: | --- |
| BAD | ~~Setting PowerLevel to 9000 is required, because efficiency is better than reliability in high-rate situations.~~ |
| GOOD | **In high-rate situations (≥1k req/s), PowerLevel should be set to 9000 in order to maximize efficiency; otherwise, the system defaults to ensuring reliability.** |

## Tip #3: Focus on concrete ideas.

It can be easy to get sidetracked within documentation, or to bury critical points amidst tangent,
ancillary, or otherwise extraneous information. When writing documentation, always ask, "What am I
trying to say?". [Using terse bullets][skeleton] can help structure information, and using a
[self-defined guideline for sentence (or paragraph) length][concise-sentences] can help to optimize
the message such that the information density is high (i.e., no fluff or filler).

## Tip #4: Use concise sentences (or fragments).

Sentences and sentence fragments should be simple and to the point (but not stuttering). Each
sentence should ideally convey a single idea, but clearly-structured compound sentences are
acceptable as well.

| Examples | |
| :-: | --- |
| BAD | ~~In order to time travel the MaxSpeed setting must be set to 88MPH because lower speeds will not achieve the desired result, but users should be careful around turns as high speeds are dangerous.~~ |
| BAD | ~~Time travel requires MaxSpeed to be set. The ideal value is 88MPH. Lower speeds will not achieve the desired result. Users should be careful around turns as high speeds are dangerous.~~ |
| GOOD | **Setting MaxSpeed to 88MPH is required to enable time travel – lower speeds will not achieve the desired result. However, users should take care around turns, as high speeds are dangerous.** |

## Tip #5: Write down questions that the documentation should answer.

The point of writing documentation is to document the subject and educate the reader, but every
piece of documentation should be written with an understanding of which questions it is answering.
Questions can be as specific as "How do I deploy service X?", or as vague as "How does the system
work?", but for vaguer questions, there are ostensibly sub-questions (or
[skeleton bullets][skeleton]) that help to flesh them out.

These questions can be included as a preface to the documentation if desired, which can serve to set
the reader's expectations and frame of reference (both of which can help to improve comprehension
and retention).

A contrived example:

```
After reading this document, users should be able to answer:

    - What does the system's topology look like?
    - What are components X, Y, and Z, and how are they related?
    - When should one use Modes 1-3 of component X?
    - How can component Y be deployed in a HA capacity?
    - How can components X and Y be tuned to mitigate load on component Z?
    - How can all of the components be configured for correctness versus availability?
```

## Tip #6: Write skeleton documentation first.

Starting with a skeleton shows the full breadth and depth of a document (or set of documents) before
anything is written, enabling a more holistic view of the content and helping to ensure cohesion and
flow. Conversely, by fleshing out each section as it's added, it can be easy to lose track of the
scope of the section relative to the document, or the scope of the document itself. This approach
also serves as a TODO list, helping to ensure that sections aren't forgotten about and enabling the
documentation to be divided and conquered.

A contrived example:

```
# The Document Title

## Overview

    - One-sentence or elevator pitch summary
    - Introduce components X, Y, and Z

## Component X

### Overview
    - Responsible for all client ingress
    - Sole entrypoint into the system
    - Can be deployed to support multiple client encodings
    - Adapter layer for translating all ingress from clients into <format>
    - Traffic goes: Client -> Component X -> Component Y -> Component Z

### Deployment modes
    - HA
    - Lossless
    - High-throughput

## Component Y

### Overview
    - Dynamic, stateless, full-mesh cluster topology
    - Deduplicates all ingress from Component X
    - Partitions data among Component Z backends using perfect hashing

### Deployment modes
    - Dynamic cluster
    - Static cluster
    - Full mesh
    - Ring network
    - Isolated

## Component Z

### Overview
    - Persists normalized client data
    - Maintains a sharded, in-memory cache

### Deployment modes
    - HA
    - Prioritized throughput
    - Writethrough cache
    - Readthrough cache
    - Ring buffer
    - LRU
```

## Tip #7: When in doubt, less is more.

While it may seem counterintuitive to start by writing less in a given document, terseness –
specifically, fewer sentences or words to explain an idea – is easier to fix than verboseness.
It is far easier to find gaps in a reader's comprehension that are caused by missing documentation
than it is to try and find the minimal subset of documentation necessary to convey an idea. In other
words: start short, and build out.

| Examples | |
| :-: | --- |
| BAD | ~~Configuring the cache to be a LRU readthrough cache ensures that the minimal amount of memory is used at the expense of guaranteed initial cache misses. Using minimal memory is important, as servers have finite memory that does not scale with the size of persisted data, and thus the bulk of the data will incur cache misses after a certain point (though compression may be used to mitigate this in the future).~~ |
| GOOD | **Configuring the cache as a LRU readthrough cache results in a minimal memory footprint (due to the combined behavior of both readthrough and LRU caching) at the expense of initial cache misses. Cache efficiency may be improved in the future.** |


## Tip #8: Write for an uninitiated target audience (and call out prerequisites).

Every piece of technical documentation has at least one baseline prerequisite that the documentation
builds upon; otherwise, every document would need to explain
[how computers work][how-computers-work]. However, in the case of M3 (as an example), the reader
might be someone writing code that emits metrics, an infrastructure engineer that wants to run a
metrics stack, someone with experience running other metrics stacks that is looking for nuanced
tradeoffs between this and other platforms, or a distributed systems enthusiast (or a thousand other
folks).

It's relatively straightforward to tailor a document to a given audience: an end-user looking to
emit metrics is a _much_ different demographic than someone wanting to run M3 in their
infrastructure (etc), and as such, there is often no conflation. However, the skill level or
technical depth of a given audience is often varied: a person who only vaguely understands what a
metric really is will require a much broader set of information than a person who has experience
using metrics in complex and interesting ways.

Thus, it's important to cater to a baseline: don't expect that every piece of technical jargon will
be understood. Similarly, it's not a document's responsibility to satisfy its own prerequisites -
it's okay to inform the reader that they should be familiar with X, Y, and Z, else they won't get
much out of the document. If there are prerequisites that normalize the baseline of most or all
readers, call that out at the beginning (e.g., "Users should read and understand Document X before
reading this document").

## Tip #9: Make sure that documentation is reviewed thoroughly.

Erroneous documentation can lead to confusion, misunderstanding, incorrect assumptions, or – in the
worst case – a user unknowingly implementing bad or faulty behavior. Documentation should ideally be
reviewed like code: thoroughly and pedantically. Depending on the complexity, it may be worth
[asking reviewers to answer basic questions][write-down-questions] and update the documentation
based on their responses – for example, if the question "When should `ConfigValueX` be set, and
what are its side effects?" can't be answered, it might merit adding more documentation around the
semantics of `ConfigValueX`, as well as its direct and side effects.

## Tip #10: Have _at least_ one less-familiar person review documentation.

It's _extremely_ easy to let assumptions, implicit points, jargon, etc sneak in. Folks who are
familiar with a subject are more likely to subconsciously or contextually fill in the gaps, but they
are not the documentation's intended audience. Instead, try to find one or more folks who are closer
to the target audience (relatively speaking) review documentation, and evaluate their comprehension
(e.g., ask basic questions that the documentation should provide answers for).

Reviewers should sanity check all assumptions, and ask any questions freely – it's important to not
assume that they just missed the boat on some critical information that is obvious to everyone else
(most documentation should be explanatory and not expect readers to continually read between the
lines).

## Tip #11: Document both reality _and_ intent.

While documentation should reflect the current state of the world, in many cases, it's also
important to document the context or rationale around that state (the "what" and the "why").
Documenting only the "what" leaves readers to draw their own inferences and conclusions, while the
"why" answers many of those questions outright (or, at the least, imparts a line of thinking that
will more accurately inform those inferences).

## Tip #12: Focus on readability.

The goal of documentation is to provide information to the reader. This means that, aside from
correctness, the most important metrics of documentation are its efficiency (words per idea) and
efficacy (readability of words and retention of ideas).

Reading a sentence in isolation helps to analyze the readability of an idea in a narrow context,
but reading multiple sentences (or a paragraph) at a time can be a good signal of how well
information flows.

## Tip #13: Be pedantic about grammar.

English grammar can often be flexible, subjectively readable, or confusing (there is more than one
way to skin a cat). Being clear and using [simpler, well-structured sentences][concise-sentences]
can [help with readability][readability] by presenting digestible, unambiguous ideas.
[Verb tense][verb-tense] and [pronouns][pronouns] can play an important role as well. Ultimately,
the best litmus test may be the simplest: whether a reader needs to read a sentence twice to
understand it (technical terms notwithstanding).

Realistically, it is impractical to strive for or expect perfect grammar. The goal isn't to write
flawless English; the goal is to write _clear, concise, unambiguous, and readable_ documentation,
for which grammar is an important (albeit not the only) tool.

## Tip #14: Use verb tenses consistently.

Generally speaking, the bulk of documentation only makes sense when written in the present tense
(its "common tense", so to speak), as objects are typically referred to in the abstract (e.g.
"Component X" is abstract, "The running instance of Component X" proverbially concrete) and do not
usually involve time (e.g. "Component X is an upstream of Component Y" refers to nominal abstract
state, "Component X will be an upstream of Component Y" is prospective state). Of course, this is
not always true: documentation can tell a story, and tenses should be used as needed to express ideas
relative to time.

However, cases of general fact, such as describing cause and effect:

    Value X -> Behavior Y

should be described in the present tense:

    Setting Value X causes Behavior Y.

versus the future tense:

    Setting Value X will cause Behavior Y.

in order to be consistent with the documentation's common tense, to maintain a time-less (and thus
ongoing) objective truth, and to contrast actual time-relative statements (e.g. "Setting Value X
causes Behavior Y, but will be updated to cause Behavior Z in the future").

## Tip #15: Make consistent references.

When communicating verbally, there is no need to distinguish between represented forms for a given
artifact, value, method, etc - one can simply say "foo bar" to reference "the member function or
property `Bar` on type `Foo`", or "foo dot bar" to reference "the YAML key `bar` nested under
the key `foo`" (as simple examples).

In written documentation, however, these references can either maintain continuity throughout a
document (when used consistently), or they can cause confusion (when used inconsistently). A simple
framework for references is, "if it's code (or code-like), it should look like code; otherwise,
treat it like a proper noun". Thankfully, most references fall into two such categories:

- **Code (and code-like) references**, such as `MyClass` or `myVar` or `some.config.property`. In
  these cases, `PreformattedText` should be used, and should reference the code verbatim. All
  instances of such a reference should be consistent, and should not be abbreviated.
- **Proper (or effectively proper) names**, such as "My Component" or "My System Name". In these
  cases, "Title Case" should be used, with the following exceptions:
    - Established projects, such as "etcd", "PostgreSQL", and "GitHub", should use the project's
      official name and format, versus arbitrary ones (e.g. "Etcd", "Postgres", "Github", etc).
    - First-party components, such as "the M3 Aggregator", can use shorthand when doing so does not
      contextually result in ambiguity (e.g. "the Aggregator", when already discussing the Aggregator),
      but should still be proper nouns.

For other or nuanced cases, it's more important to be consistent than it is to fit a reference into
one of these simple categories.

| Examples | |
| :-: | --- |
| BAD | ~~The Flux Capactitor is a Y-shaped component that enables time travel. Importantly, the enclosing vehicle must be capable of providing 1.21 gigawatts of power, as the flux capacitor requires this energy to function.~~ |
| BAD | ~~FluxCapacitor's power level property should be set to 1.21.~~ |
| GOOD | **The Flux Capacitor is a Y-shaped component that enables time travel. Importantly, the enclosing vehicle must be capable of providing 1.21 gigawatts of power, as the Flux Capacitor requires this energy to function.** |
| GOOD | **`FluxCapacitor`'s `powerLevel` property should be set to `1.21`.** |

## Tip #16: Use `TODO`/`TODOC` placeholders liberally.

Often, it might not yet be the right time to write a part of the documentation – either because a
feature isn't ready yet, it's not the most practical time investment at present, or other reasons.
In these cases, consider adding a `TODO` (in exactly the same way that code `TODO`s are used). If
it's necessary/helpful to disambiguate code `TODO`s from documentation `TODO`s, `TODOC` can be used
instead: it's similar enough to match a naive `TODO` search, but both `TODO` and `TODOC` can be
reasoned about independently as desired. For example:

```go
// FluxCapacitor TODOC(anyone)
type FluxCapacitor struct {
    // TODO(anyone): use a discrete GigaWatts type instead of a float
    RequiredPower float64
}
```

```shell
$ grep -rnH 'TODOC(' .
./time/timetravel/flux_capacitor.go:123: // FluxCapacitor TODOC(anyone)
```

```shell
$ grep -rnH 'TODO(' .
./time/timetravel/flux_capacitor.go:125:     // TODO(anyone): use a discrete GigaWatts type instead of a float
```

[concise-sentences]: #tip-4-use-concise-sentences-or-fragments
[how-computers-work]: https://softwareengineering.stackexchange.com/a/81715/4860
[pronouns]: #tip--1-avoid-first-andsecond-person-pronouns
[readability]: #tip-12-focus-on-readability
[skeleton]: #tip-6-write-skeleton-documentation-first
[verb-tense]: #tip-14-use-verb-tenses-consistently
[wp1p]: https://en.wikipedia.org/wiki/Narration#First-person
[wp2p]: https://en.wikipedia.org/wiki/Narration#Second-person
[wp3p]: https://en.wikipedia.org/wiki/Narration#Third-person
[wp3ponv]: https://en.wikipedia.org/wiki/Narration#Third-person,_objective
[write-down-questions]: #tip-5-write-down-questions-that-the-documentation-should-answer
