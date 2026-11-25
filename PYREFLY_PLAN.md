# Pyrefly adoption: static typing at the Celery–FireX boundary

## Status and governing decision

Original investigation snapshot: **2026-11-19**. The original inventories and
experiments are retained as historical evidence, not a current diagnostic
baseline or completion report. Subsequent compatibility work does not mean the
larger boundary proposal has been completed. The scope revision below governs
further implementation; the latest verified environment and results are the
**2026-11-24 production-only setup checkpoint in section 1**.

The team's governing decision is:

> FireX should expose clean, accurate typing contracts. Celery-specific typing
> problems should be resolved at the Celery–FireX boundary, not propagated into
> services and application code.

This document supersedes the earlier phase-2 implementation proposal. That
implementation was reverted and must not be reapplied unchanged.

The migration's eventual goal remains **zero errors and zero warnings in strict
mode**, without concealing incompatibilities behind broad `Any`, ignores, or a
baseline. Production code is the first milestone; tests must eventually be
included before claiming the entire repository is type-clean.

### Scope revision — 2026-11-23: static Pyrefly checking only

**The current effort is exclusively static typing with Pyrefly. Runtime type
checking is deferred to a separate, much later effort and is not a prerequisite
for completing this migration.**

In scope:

- Accurate annotations, generics, overloads, protocols, and typed data shapes for
  existing FireX behavior and APIs actually used by FireX.
- Correct third-party stubs and small, centralized typing adaptations at the
  Celery–FireX boundary, without leaking framework typing problems to consumers.
- Strict Pyrefly diagnostics and type-level consumer/regression checks, including
  examples that the checker must reject.

Out of scope:

- Adding runtime type checks for task arguments, results, BOG values, or
  deserialized data.
- Introducing or expanding Pydantic validation/coercion, annotation-driven
  runtime enforcement, or a runtime type-checking framework.
- Redesigning task execution, canvas behavior, or backend lifecycle to introduce
  those checks.

Existing validators and runtime behavior must remain intact. Studying runtime
code to write truthful static types, preserving import/annotation compatibility,
and running ordinary behavioral regression tests are not new runtime type
checking. A clean Pyrefly result does not mean dynamic input data is validated
during execution.

Constraints carried forward:

- Defer general static typing cleanup in `tests/`; focused type-level regression
  examples and relevant existing behavioral tests are still appropriate.
  Shipped testing APIs inside production packages remain in scope.
- Do not integrate Pyrefly into CI until most existing debt has been resolved
  and enforcement has been revisited explicitly.
- IDE integration is not a goal.
- Keep unrelated refactoring and application cleanup out of boundary batches.
- Preserve existing runtime behavior unless an actual behavior correction is
  explicitly identified, reviewed, and tested as such.

## 1. Environment and diagnostic inventories

### Current checkpoint — 2026-11-24, production-only typing setup

Verified on branch `pyrefly-celery-boundary`, source HEAD `8b88f1d`, with the
user's Celery upgrade in `pyproject.toml` and `uv.lock`. The initial upgrade
rerun changed no source, dependencies, lockfile, or checker configuration. The
worker-loss compatibility follow-up changed only the in-memory test backend and
its regression tests. This setup batch installs the declared test dependency
group and narrows the default checker scope. It does not change dependency
pins, the lockfile, or runtime source code.

| Component | Installed version/source |
| --- | --- |
| Python | 3.10.15 |
| firexapp | 5.7.244 |
| Celery | 5.7.0 |
| Kombu | 5.7.0 |
| Billiard | 4.3.2 |
| AMQP | 5.4.2 |
| click-repl | 0.4.1 |
| Redis client | 6.4.0 |
| celery-types | 0.26.0, locally built wheel, not the PyPI artifact |
| Pyrefly | 1.4.1 |
| Pydantic | 2.13.0 |
| typing-extensions | 4.16.0 |
| Coverage | 5.5, installed from the existing `test` dependency group |

The temporary `[tool.uv.sources]` entry still selects
`../../../celery-types-fix/celery-types/dist/celery_types-0.26.0-py3-none-any.whl`.
Installed stub/marker files were checked against that wheel. The version number
alone does not distinguish it from the older PyPI package. This machine-local
override still needs a portable dependency source before the FireX change is
ready to merge.

#### Refreshed strict Pyrefly inventory

The repository's `[tool.pyrefly]` configuration now checks the same seven
production packages used in the original investigation by default. General
`tests/` and docs typing remain deferred; shipped helpers in `firexapp/testing/`
and `firexkit/testing.py` remain included. The resolved file list contains
exactly **93 modules**, all under the seven shipped package roots.

The configuration keeps `preset = "strict"` and explicitly sets
`min-severity = "warn"`. It targets the project's minimum Python version,
3.10, queries `.venv/bin/python`, and uses the repository root as its explicit
import search path with search-path heuristics disabled. All paths are
checkout-relative, not machine-specific absolute paths. See the
[Pyrefly configuration reference](https://pyrefly.org/en/docs/configuration/)
for these options. No diagnostic suppressions or checker baseline file were
introduced.

| Scope/checkpoint | Modules | Errors | Warnings | Total |
| --- | ---: | ---: | ---: | ---: |
| Last production checkpoint on Celery 5.6.3, after local stub/InjectArgs fixes | 93 | 2,121 | 17 | **2,138** |
| Celery 5.7 production scope, before installing the test group | 93 | 2,121 | 17 | **2,138** |
| Current default production scope, with the test group installed | 93 | 2,120 | 17 | **2,137** |
| Earlier initialized configuration, including tests/docs, before the test-group installation | 178 | 3,846 | 33 | **3,879** |

Upgrading Celery alone did not change the production diagnostic set. Installing
the test group now removes the missing `coverage` import error; it also refines
an existing `os.execlp` argument-error message in `coverage_plugin.py` from
`str | Unknown` to `str`. That argument error remains unresolved. Switching
from the temporary production configuration to the checked-in configuration
changes no diagnostic messages or multiplicities. No tests/docs diagnostics
were counted as fixed by narrowing the default scope.

| Current production package | Diagnostics, including warnings |
| --- | ---: |
| `firexapp` | 737 |
| `firexkit` | 699 |
| `firex_flame` | 452 |
| `firex_keeper` | 168 |
| `firex_blaze` | 46 |
| `firex_bundle_ci` | 35 |
| `firex_flame_ui` | 0 |
| **Total** | **2,137** |

The leading categories remain missing parameter annotations (1,362), missing
generic arguments (224), and missing attributes (177). All **14** dictionary
`.s()` diagnostics and **91** missing `ARR` arguments for `FxAsyncResult` remain.
The declared `test` dependency group was installed with a locked, inexact sync
that did not reinstall the project. Only `coverage==5.5` was added; all existing
installed dependency versions and the local celery-types wheel were preserved.
Coverage 5.5 has no `py.typed` marker, but the only package attribute used by
`coverage_plugin.py`, `coverage.__file__`, resolves as `str` in a strict static
consumer probe with zero diagnostics. No Coverage upgrade or extra stubs were
needed for that existing use.

#### Focused and compatibility checks

| Check | Result |
| --- | --- |
| Signal adapter and its focused signal tests, checked with strict Pyrefly | 0 diagnostics |
| Writable `InjectArgs.options` static consumer probe | 0 diagnostics |
| Native generic-base/static factory consumer probe | 0 diagnostics |
| Coverage module-file static consumer probe | 0 diagnostics |
| Dependency compatibility, lock freshness, inexact/no-project-install sync dry run | Pass; sync would make no changes |
| Ruff lint/format on the eleven boundary/backend implementation/test files | Pass |
| Combined ten focused behavioral modules, including backend helper/consumer tests | **263 passed; 69 subtests passed** |

The initial upgrade run had **152 passed, 14 failed; 69 subtests passed** across
the original five focused modules. All 14 failures were in
`tests/firexkit_unit_tests/task_request_tests.py` and shared the worker-loss path
introduced by Celery 5.7:

- `Request.on_failure()` now calls `_success_was_stored()` for `WorkerLostError`.
- That path reads `backend.max_retries` before its guarded metadata lookup.
- The in-memory `firexkit.testing.UtBackend` stand-in lacked that attribute, so
  the call raised `AttributeError`; the pool-callback case also failed to record
  the expected failure state as a consequence.
- The lookup also uses backend internals `_ensure_retryable` and
  `_get_task_meta_for`; adding only `max_retries` would let the guarded lookup
  silently fall back to failure instead of preserving an already-stored success.

The compatibility fix supplies all three parts on `UtBackend`:

- `max_retries = 0`, since the in-memory backend does not retry transport errors.
- `_get_task_meta_for()` presents the existing stored state as Celery metadata,
  defaulting to `PENDING` when absent. The stand-in still stores only states,
  not result payloads; metadata therefore contains `result=None`.
- `_ensure_retryable()` consumes Celery's retry-control keywords and invokes
  the supplied operation once, forwarding its arguments and propagating its
  errors. This matches a Celery backend with `always_retry` disabled; it does
  not emulate network retries or translate errors into fallback exceptions.

New regression tests cover metadata states, argument forwarding/error
propagation, preservation of stored `SUCCESS` with early and late
acknowledgements, and normal worker-loss failure recording when metadata cannot
be read. The original failing tests and their assertions remain in place.
No production worker-failure handling, checker suppressions, or test skips were
introduced. The backend compatibility fix itself changed neither diagnostic
inventory, apart from source line shifts; the subsequent test-group installation
accounts for the production inventory delta recorded above.

This identifies an upgrade/test-double incompatibility, not proof of the same
failure in the production Redis backend. The ten-module rerun still emits three
`DuplicateTaskNameWarning` warnings from repeated-registration plugin tests.
These are behavioral-test warnings, separate from the 17 production Pyrefly
warnings. Four of those modules exercise pending children, forgetting,
skip-forget indexes, and results; this is not a full-repository test run.

Raw reports, exact commands, environment metadata, and comparison results are
under `/tmp/.smx-bkrzyk/firex-celery57-checks-h6j69q2o` (or `/host_tmp` equivalent).
Compatibility-fix test logs and before/after diagnostic comparisons are under
`/tmp/.smx-bkrzyk/firex-celery57-tests-uu43zuoo` (or `/host_tmp` equivalent).
The production-scope/setup inventory, resolved file list, dependency and wheel
verification, and refreshed test logs are under
`/tmp/.smx-bkrzyk/firex-pyrefly-setup-94b9dk4g` (or `/host_tmp` equivalent).
The durable results are recorded here; temporary artifacts are not a portable
dependency of the plan.

### Original investigation — 2026-11-19

Repository: `firexapp`, branch `master`, HEAD
`0e38d5a36829a98ee0c770d641bba6a32d76aa3b` at investigation time.
There were existing user changes in `pyproject.toml` and `uv.lock`; the
investigation preserved them.

| Component | Observed version |
| --- | --- |
| Python | 3.10.15 |
| firexapp | 5.7.244 |
| Celery | 5.6.3 |
| celery-types | 0.26.0 |
| Pyrefly | 1.4.1 |
| Pydantic | 2.13.0 |
| typing-extensions | 4.16.0 |

### Scope and counts

At the investigation snapshot, the initialized configuration included
`**/*.py*` and `**/*.ipynb` with `preset = "strict"`, including tests. The
inventory below explicitly includes warnings using `--min-severity warn`.

| Checked scope | Modules | Errors | Warnings | Total |
| --- | ---: | ---: | ---: | ---: |
| Initialized configuration | 176 | 3,867 | 66 | **3,933** |
| Seven production packages only | 92 | 2,143 | 42 | **2,185** |

The initialized inventory includes **1,747 diagnostics in `tests/`** and one in
docs. A separate temporary configuration was used to investigate production
code without changing repository configuration.

| Production package | Diagnostics |
| --- | ---: |
| `firexapp` | 753 |
| `firexkit` | 719 |
| `firex_flame` | 462 |
| `firex_keeper` | 169 |
| `firex_blaze` | 47 |
| `firex_bundle_ci` | 35 |
| `firex_flame_ui` | 0 |
| **Total** | **2,185** |

The largest diagnostic categories are missing parameter annotations (1,367),
missing generic arguments (224), and missing attributes (180). Counts are an
inventory, not an estimate of effort or proof of contract quality.

These counts should not be treated as a direct before/after comparison with
the November 14 migration: source, environment, dependencies, and configuration
have changed since then.

### Environment gaps in the original inventory

The production check reported 25 untyped-import diagnostics across `psutil`,
`gevent`, `paramiko`, `entrypoints`, and `pytz`, plus a missing `coverage` import
from shipped `firexapp/testing/coverage_plugin.py`.

The earlier phase-1 environment work was not present in this checkout. These
gaps are independent of Celery. Do not upgrade runtime dependencies merely to
satisfy a newer stub package.

### Dependency setup result — 2026-11-19

Added the following exact-pinned stubs to the main dependencies, as requested:

| Dependency | Pin | Installed runtime |
| --- | --- | --- |
| `types-entrypoints` | `0.4.1.20260408` | `entrypoints==0.4` |
| `types-gevent` | `26.8.0.20260811` | `gevent==26.8.0` |
| `types-paramiko` | `5.0.0.20261021` | `paramiko==5.0.0` |
| `types-psutil` | `7.2.2.20261111` | `psutil==7.2.2` |
| `types-pytz` | `2026.3.1.20260727` | `pytz==2026.3.post1` |

`types-greenlet==3.5.0.20260518` was added transitively through `types-gevent`.
All 41 main dependency declarations are now alphabetized by package name and
exact-pinned. Existing runtime pins use the versions already installed locally.
The dev and test groups are exact-pinned too. Optional docs packages use their
existing locked versions, retaining separate Sphinx pins for Python 3.10, 3.11,
and 3.12+. The build-system version constraint was not changed.

All 88 previously installed distributions retained their versions. All 111
pre-existing lockfile package version/source records were preserved; the six
new stub packages bring the installed count to 94 and the lockfile count to 117.

| Diagnostic inventory | Before stubs | After stubs |
| --- | ---: | ---: |
| Production untyped imports | 25 | 0 |
| All-scope untyped imports | 33 | 0 |
| Production errors | 2,143 | 2,143 |
| Production warnings | 42 | 17 |
| **Production total** | **2,185** | **2,160** |
| **Initialized-scope total** | **3,933** | **3,900** |

Lockfile validation, environment sync checks, installed dependency compatibility,
dependency/FireX import smoke checks, and all six argument-conversion unit tests
passed. No production source, tests, CI, build configuration, or Pyrefly settings
changed. Existing code diagnostics remain; the checker is not yet green.

At this November 19 checkpoint, `coverage==5.5` was declared in the test group
but not installed; the November 24 setup above resolves that gap. The original
all-scope check also reported three local plugin-fixture imports; those are
separate test-scope/import-root concerns, not missing PyPI stubs.

### Original celery-types setup findings (Celery 5.6.3)

- The installed stub package satisfies its Python and typing-extensions
  requirements. Dependency compatibility checks passed during setup review.
- Pyrefly discovers the installed stubs when pointed at this repository's
  `.venv`. No checker plugin, `import celery_types`, or manual stub-path
  configuration is needed for the current installation.
- The package provides stubs for Celery and related packages, including Kombu,
  Billiard, AMQP, and Vine. It does not automatically patch runtime classes.
- Installation alone does not make `Task[...]`, `Celery[...]`, or
  `AsyncResult[...]` valid runtime expressions on the original Celery 5.6.3
  runtime; `Signature[...]` already worked. **The installed Celery 5.7 upgrade
  removes this particular limitation**, as verified in the current checkpoint.
- Type-checking-only base aliases were a compatibility option for Celery 5.6.3.
  For the current 5.7 baseline, prefer native generic bases instead; use
  `TYPE_CHECKING` only where still needed for actual import dependencies or
  annotation compatibility. No runtime generic monkeypatch is needed.
- Deferred annotations do not solve evaluated base classes or later annotation
  evaluation. FireX inspects service annotations at runtime; types needed for
  its existing behavior must remain resolvable. This is a compatibility
  constraint, not a requirement to add or expand runtime argument validation.

See the [celery-types 0.26.0 installation instructions](https://pypi.org/project/celery-types/0.26.0/#description)
and [Pyrefly import resolution](https://pyrefly.org/en/docs/import-resolution/).

## 2. Classify problems before choosing a remedy

| Problem category | Example | Appropriate response |
| --- | --- | --- |
| Stub inaccurately describes supported runtime behavior | Writable signature fields declared read-only | Correct the stub or provide a small, versioned compatibility adaptation |
| FireX intentionally differs from ordinary Celery | Function returns a scalar; FireX execution returns a result dictionary | Model the actual FireX contract statically at the boundary |
| Runtime guarantee is conditional | Redis backend exists only in appropriate app modes | Represent the actual alternatives and existing lifecycle guarantees in static types; do not promise unconditional Redis access or add runtime validators |
| FireX implementation has a real bug | `retry(throw=False)` drops the parent's return value | Record separately; any behavior fix requires its own explicit scope and regression coverage |
| Application code lacks its own data types | Unknown structure of a business payload in BOG | Type the business data; do not attribute it to Celery |

The installed runtime and supported FireX behavior are authoritative when a
stub is demonstrably inaccurate. Conversely, a runtime bug must not be excused
as an inaccurate stub simply because a typing change could silence it.

The remedy in this phase is an accurate static model, not new runtime type
enforcement. If a behavior discrepancy blocks a truthful static contract,
identify it explicitly rather than silently expanding the migration's scope.

## 3. Task creation: reuse the new generic support, with corrections

Source: [FireXCelery](firexkit/firex_celery.py), particularly `task()`.

### Confirmed root cause of the dictionary `.s()` errors

The current forwarding override declares every call to return `FireXTask`:

```python
def task(self, *args, **kwargs) -> FireXTask:
    return super().task(*args, **kwargs)
```

But these are distinct operations:

```python
app.task(function)  # Task creation, possibly returning a lazy proxy.
app.task(**options)  # Decorator creation; no task has been supplied yet.
```

For `@app.task()`, Python applies `app.task()(function)`. With the current
annotation, Pyrefly treats that second call as `FireXTask.__call__`, whose return
type is a dictionary. The decorated service therefore appears to be a dictionary
without `.s()`.

### Historical controlled experiment on Celery 5.6.3

The new stubs support `Celery[CustomTaskType]`. In a temporary copy, specializing
that task type to `FireXTask` and removing FireX's forwarding-only override:

- Removed all **14** dictionary `.s()` diagnostics.
- Inferred `FireXTask` for direct and ordinary decorator-factory creation.
- Inferred `type[FireXTask]` for `app.Task`.
- Rejected a non-callable input and two positional callable inputs.
- Did not need the permissive variadic overload from the previous proposal.

The experiment used a type-checking-only base alias; the runtime base remained
the original Celery class. This is a promising foundation, not a complete patch.

### Current direction after the Celery 5.7 upgrade

Celery 5.7's native generic support allows evaluated expressions and subclass
bases using `Celery[...]`, `Task[...]`, and `AsyncResult[...]` without a
monkeypatch or a conditional base alias solely to avoid subscription errors.
This is typing-syntax support, not execution-time type validation.

An isolated strict Pyrefly consumer using `class TypedFireXApp(Celery[FireXTask])`
passes exact type assertions for `app.Task`, direct task creation, and ordinary
and named decorator-factory creation. Native generic expressions and subclass
bases also work in the installed Python 3.10/Celery 5.7 environment.
See the [5.7 generic tests](https://github.com/celery/celery/blob/v5.7.0/t/unit/test_generics.py).

Use this simpler foundation for the next task-factory typing batch. It has
**not** been applied to the production `FireXCelery`, `FireXTask`, or
`FxAsyncResult` declarations. The inaccurate forwarding override and supported
option/return contracts still need deliberate typing work; the passing probe
is not a completed boundary implementation.

Celery also replaced its root-module static-analysis hack with standard
`TYPE_CHECKING` imports in [PR #10409](https://github.com/celery/celery/pull/10409).
Keep using the corrected stubs: this export cleanup does not replace their
contracts or infer FireX's result envelope automatically. Pydantic's new model
argument/serialization behavior remains outside this static-only migration.

### Historical factory-call discrepancies in the installed stubs

| Form | Observed runtime | Stub behavior |
| --- | --- | --- |
| `app.task()(fun=func)` | Accepts the named function argument | Rejects `fun` and reports a missing positional argument |
| `app.task(func, lazy=False)` | Accepts callable plus options | Reports no matching overload |
| `app.task(fun=func)` | Returns a decorator, not an immediately created task | Infers a task |

At investigation time, the inner `fun=` form was used by plugin replacement in
[plugins.py](firexapp/plugins.py). Its unannotated `fx_app` parameter hid the
mismatch in the full scan; a strongly typed consumer probe exposed it. FireX
has since changed that call to pass `func` positionally. These historical
findings do not require restoring the keyword form or adding decorator helpers
for forms FireX does not use.

`Callable[[FunctionType], FireXTask]` cannot preserve that named parameter. A
callback protocol with an explicitly named `fun` parameter is one applicable
tool, but the complete factory contract must also account for the parent API.

Other options require explicit policy:

- `base=` can select a task class that is not a FireX task.
- `filter=` can replace the returned task with an unrelated value.
- Lazy task proxies must retain existing construction/finalization behavior.
- The outer `fun=` keyword must not be mistaken for direct task creation.

Do not blindly inherit inaccurate overloads, rebuild a permissive catch-all to
appease override checking, or promise that every supported Celery option always
produces `FireXTask`.

## 4. Separate raw function returns from FireX execution results

Sources: [FireXTask.real_call](firexkit/task.py) and
[FireXResults](firexkit/result.py).

FireX performs a real conversion between calling the original function and
producing an execution result. An in-process synthetic task demonstrated:

```text
Original function result: 7
FireX task-call result:   {"value": 7, "__task_return_keys": ("value",)}
```

These contracts must remain distinct:

1. The original function's inputs and raw return type.
2. FireX's execution-result envelope, including its BOG/result processing.
3. The asynchronous/eager result handle and its lifecycle.
4. Dictionary or tuple extraction performed by FireX result APIs.
5. An individual business value extracted from the result.

Do not propagate the raw function return type into the asynchronous FireX result
as though FireX were ordinary Celery. Do not change a service function's truthful
scalar/tuple return annotation to a dictionary merely to describe the envelope.

BOG also makes partial signatures and argument injection intentional. Static
`ParamSpec` argument checks must not falsely require all service inputs when
constructing a partial signature. No execution-time argument checking is added.

`@returns`, `@flame`, and `@flame_collapse` attach metadata and preserve the
original callable. Their typing should preserve its parameters and return type,
without changing function identity, decorator-order checks, or metadata storage.

Heterogeneous BOG values still require deliberate data typing. Containing
framework uncertainty does not automatically infer arbitrary business schemas.
Distinguish a justified dynamic payload from an accidental `Any` framework API.

## 5. Result handles: eliminate the phantom generic burden

Source: [FxAsyncResult](firexkit/result.py).

Current declaration:

```python
class FxAsyncResult(AsyncResult, Generic[ARR]): ...
```

`ARR` is not connected to the inherited AsyncResult payload parameter and has no
other use in this module. A typed probe produced:

```text
Given result: FxAsyncResult[int]
result.get()          -> Unknown
result.result         -> BaseException | Unknown
result.get_results()  -> dict[str, Any]
```

Nevertheless, bare `FxAsyncResult` annotations cause **91 missing-type-argument
diagnostics** across production code. There are no explicit `FxAsyncResult[...]`
usages in the checked production packages or tests.

Do not solve this by adding `FxAsyncResult[Any]` to every consumer. Decide between:

- A concrete standard FireX result handle for the execution envelope; or
- A meaningful generic connected to the actual payload, with a useful default
  if omission is intended to remain supported.

The initial preference is a concrete standard handle unless a real multiple-
payload requirement is identified. External consumers still need a compatibility
audit before removing a public subscription capability.

Normal service code should use FireX's waiting/extraction APIs, not bypass them
with raw Celery `get()`. The `get()` probe above diagnoses the disconnected type
parameter; it is not a recommendation to use that method in services.

Further result-boundary work includes:

- Parent/child result reconstruction and eager versus asynchronous results.
- Pending, successful, failed, revoked, forgotten, and backend-detached states.
- Success payloads versus stored exceptions and absent results.
- Accurate dict/tuple extraction contracts; `legacy_extract_results` currently
  promises a dictionary despite delegating to an API that can return a tuple.
- Serialization/copy behavior. The installed native AsyncResult implementation
  returns five reduce arguments, while its stub declares a two-element tuple.
- Mutable inherited fields: narrowing `app` or `parent` on a subclass is not
  automatically a valid substitution for Celery's writable attributes.

## 6. Apps, registries, configuration, and backend capabilities

Sources: [FireXCelery](firexkit/firex_celery.py),
[FireXTask](firexkit/task.py), and [FxAsyncResult](firexkit/result.py).

Specializing Celery's task parameter does not specialize all of its APIs:

```text
app.tasks["name"] -> Task[..., Any]
app.AsyncResult() -> AsyncResult[Any]
```

The installed `TaskRegistry` stub is not generic. FireX already has
`get_app_task()` and `get_app_tasks()` as natural normalization points. Establish
the FireX task guarantee there rather than casting every registry lookup or
pretending a registry cannot contain non-FireX tasks.

Other confirmed mismatches:

- An annotation-only `self.app: FireXCelery` in a constructor does not replace
  the inherited getter's public type. A probe still sees `Celery`.
- Runtime `Task.app` is a `celery.local.class_property`. Replacing it with an
  ordinary instance property merely for typing may change binding behavior.
- A class-level annotation alone gave the desired consumer type in an
  experiment but introduced an override diagnostic. It is not a finished fix.
- `app.conf` is a Celery `Settings` object at runtime, not the FireX configuration
  dataclass used to annotate it. A correct view must include the actual settings
  behavior and the relevant FireX configuration fields.
- `FireXCelery.backend` promises `RedisBackend`, but a valid metadata-only app
  created with `create_no_task_exec_fx_env()` has a `DisabledBackend`.
- Existing constructor annotations likewise do not turn all inherited backend
  access into Redis-specific capabilities.

Design static, descriptor-preserving contracts for access and the capabilities
available in each existing lifecycle mode. Preserve existing initialization and
guards; new runtime lifecycle/capability validation is not part of this effort.
Do not resolve these findings with unconditional Redis casts, false class
identities, or repeated lifecycle checks in business code.

Worker requests, pool jobs, bootsteps, and control APIs are also integration
boundaries, even where their code sits outside the four principal classes.
Their third-party/private fields should not require application-level typing
workarounds.

## 7. Canvas composition: define the actual common interface

Source: [SignatureX and native Signature extensions](firexkit/chain.py).

| Capability | Individual signature | Composed chain |
| --- | --- | --- |
| Runtime class | `SignatureX` | Celery `_chain` |
| `enqueue` | Present | Present |
| `verify_args` | Present | Present |
| `apply_async_x` | Present | Present |
| `enqueue_and_extract` | Present | **Missing** |

The earlier phase-2 cast of every composition to `SignatureX` was unsound. It
allowed a method call to type-check even though the composed object lacked that
method. A cast does not add methods or convert a native canvas.

Describe the supported existing FireX canvas interface statically, including
what native composition actually provides. An accurate shared interface can
model that surface; adding missing methods or introducing a new runtime canvas
implementation is not a prerequisite for this static-typing migration.

The installed stubs also erase useful information during composition:

```text
For task: Task[[int], str]
task.s()            -> Signature[str]
task.s() | task.s() -> Signature
```

Cover composition, cloning, `InjectArgs`, partial BOG inputs, groups/chords where
supported, and execution-result handling with Pyrefly consumer checks. Preserve
existing behavior without requiring casts or Celery-specific unions at every
enqueue call site.

## 8. Verified stub gaps to resolve centrally

Current disposition at the Celery 5.7 checkpoint:

- The locally installed celery-types wheel contains the prepared corrections
  for writable Signature fields/custom options, result metadata/transport
  shapes, signal lock/storage/dispatch IDs, `Celery.finalized`, and
  `Task.send_event()`. It also includes upstream's request-class support.
- `TaskSignalConnections` uses the typed signal surface directly; the focused
  Pyrefly check passes. The private sender-key helper remains isolated here.
- `InjectArgs` now initializes the inherited writable options mapping; the
  read-only override and its associated Pyrefly errors are gone.
- For the previously investigated control/request/signal exports, FireX no
  longer depends on adding those names to the stubs: control replies use
  dictionaries, request failure handling uses `ExceptionInfo`, and signal
  internals are contained in the adapter. Other integration diagnostics in the
  current inventory remain open.
- The temporary local wheel is not a portable/upstream dependency solution.
  Track that replacement separately from the remaining FireX annotation work.

The following were checked against the installed runtime and targeted probes
during the original investigation. Some have since been addressed in the
prepared celery-types fixes or FireX compatibility work; this table is historical
evidence, not a list of entirely outstanding tasks. Recheck the available stubs
before scheduling another fix.

| Stub description or omission | Observed runtime behavior |
| --- | --- |
| Signature `args`/`kwargs` are read-only | Both descriptors support assignment |
| `Signature.set()` rejects `label=` | Accepts arbitrary execution options and stores the label |
| `Task.Request` must be `str` | A custom request class is supported directly |
| AsyncResult children are serialized tuple structures | Cached serialized children are reconstructed into result objects |
| Result state excludes `REVOKED` | A revoked result reports `REVOKED` |
| Traceback is a Python traceback object | Serialized traceback text is exposed as `str` in the probe |
| AsyncResult reduce arguments have length two | Installed implementation returns five arguments |
| Several used exports are absent | `ok`, `nok`, `ExceptionWithTraceback`, `NONE_ID`, and `_make_id` exist at runtime |

Celery explicitly documents accepting a request class or its qualified name:
[custom requests](https://docs.celeryq.dev/en/stable/userguide/tasks.html#requests-and-custom-requests).

Some APIs also support custom states. Do not coerce a legitimate state into an
incorrect closed set merely to satisfy an overly narrow annotation.

Prefer upstream corrections when practical. If local support is necessary,
keep verified corrections or adapters small, versioned, and owned in one
compatibility area. Do not patch individual developer environments manually,
copy the entire third-party typing surface without need, or replace Celery with
a blanket `Any` stub.

Partial stub packages do not make every missing name inside an existing stub
module fall back to the runtime source. Installing celery-types therefore does
not by itself eliminate these missing-export diagnostics.

## 9. Historical behavior findings: separate backlog

These findings are retained as evidence, not scheduled as part of the current
static-typing work. Do not add runtime type checks to work around them or change
annotations to conceal their behavior. Any correction needs separate review and
an explicit scope decision if it blocks an accurate static contract.

### SignatureX.delay does not forward partial arguments correctly

Current implementation in [chain.py](firexkit/chain.py):

```python
return super(*partial_args, **partial_kwargs).delay()
```

An in-process probe of `sig.delay(value=9)` raised
`TypeError: super() takes no keyword arguments` before reaching the parent's
`delay` method. The parent method was mocked to prevent publication.

This is incorrect argument forwarding, not a stub limitation. A separate
behavior patch would correct the delegation and test positional arguments,
keyword arguments, and the no-extra-arguments form.

### FireXTask.retry drops the parent return value

Current override in [task.py](firexkit/task.py) calls:

```python
super().retry(*args, **kwargs)
```

without returning the result. An eager, non-publishing probe confirmed:

```text
Celery Task.retry(..., throw=False) -> Retry
FireXTask.retry(..., throw=False)   -> None
```

A separate behavior patch would need regression coverage, including ordinary
raising behavior. Do not label it `Never` or change annotations solely to hide
the non-raising path.

## 10. Related findings to keep separate from the first boundary batch

- Converter registration is annotated as accepting only `PromiseProxy`, while
  the implementation needs a task name and also works with concrete tasks.
  Model the actual requirement without introducing proxy types into consumers.
- Metadata decorators should preserve callable types and existing identity,
  wrapped-function traversal, metadata ordering, and invalid-order checks.
- FireX's `print`/`raw` logging methods need accurate contracts. They are
  installed during logging setup; a cast does not perform that initialization.
  Document the existing setup-order guarantee for static typing; adding runtime
  enforcement is outside the current scope.
- `TrackingService.start` may contribute a mapping of chain arguments or return
  `None`; its consumer explicitly merges a nonempty result. Keeper's positional
  `uid`/`install_configs` mismatch is a separate compatibility issue, not solved
  by correcting the base return type.

The latter logging/tracking work should not be bundled into the first task and
result contract patch merely because it appeared in the old phase-2 plan.

## 11. Controlled experiments and their limits

All production experiments used isolated copies, the same project interpreter,
strict mode, warning reporting, and the same seven-package scope.

| Temporary experiment | Errors | Warnings | Total | Main observation |
| --- | ---: | ---: | ---: | --- |
| Unchanged copy | 2,143 | 42 | 2,185 | Matches the production baseline |
| Specialize Celery task type; remove forwarding override | 2,127 | 42 | 2,169 | All 14 dictionary `.s()` diagnostics disappear; downstream mismatches become visible |
| Remove only the disconnected FxAsyncResult generic | 2,052 | 42 | 2,094 | All 91 missing FxAsyncResult parameter diagnostics disappear |
| Combine those two experiments | 2,036 | 42 | 2,078 | Shows the leverage of the central contracts, not a completed fix |

These are hypothesis tests, not accepted patches. In particular, they do not
resolve inaccurate factory overloads, canvas identity, backend lifecycle,
mutable-field substitution, external compatibility, or the real forwarding
bugs. A smaller diagnostic count is not proof of stronger typing.

Additional probes established:

- A strongly typed factory consumer passes for direct and ordinary configured
  creation after specialization; invalid non-callable/multiple-function calls
  are rejected.
- The named-decorator, callable-plus-options, registry, and property probes
  expose the specific remaining issues documented above.
- Lazy proxies remained initially unevaluated in the runtime audit.
- Function return conversion, writable signature fields, request-class
  resolution, native chain capabilities, and result metadata behavior were
  exercised in process.
- Runtime audit network connections were blocked. No workers, Redis instances,
  or distributed tasks were started, and no tasks were published.
- Repository-file hashes, Git diff/status, and installed package versions were
  verified unchanged after the investigation.

Original local evidence is under
`/tmp/.smx-bkrzyk/firex-celery-boundary.hthurxsp` (or its `/host_tmp` equivalent
when accessed across the tool host/container boundary). It includes JSON
inventories, temporary source variants and consumer probes, and
`runtime_boundary_audit.py` with its output. These temporary files are not a
portable or permanent artifact; the important findings are recorded here.

## 12. Active static-typing implementation sequence

- [x] Record the original baseline and investigate the listed boundary issues.
- [x] Restrict the migration to static typing with Pyrefly; defer runtime type
      checking and validation to a separate, much later effort.
- [x] Re-run both inventories and focused checks after the user's Celery 5.7
      upgrade; record the current baseline and non-green behavioral checks.
- [x] Install and verify the prepared local celery-types wheel; confirm the
      corrected signal/Signature APIs and writable InjectArgs options.
- [x] Review the Celery 5.7 `UtBackend` compatibility failures separately and
      restore a trustworthy focused-test baseline without weakening tests.
- [x] Restore the local typing environment and deliberate production-only
      scope; keep strict checks and warnings enabled. Install the declared test
      group for `coverage`, verify the local stubs, and refresh the inventory.
- [ ] Define accurate static contracts for existing public FireX task, result,
      canvas, configuration, and backend APIs before annotating their consumers
      en masse. Do not introduce runtime validators or redesign execution.
- [ ] Replace the machine-local wheel override with a portable, versioned
      dependency containing the verified third-party stub fixes before merging.
- [ ] Type the task-factory boundary using native Celery 5.7 generic bases and
      the call shapes actually used by FireX; model supported option contracts
      without a general Celery factory redesign or runtime generic monkeypatch.
- [ ] Establish typed registry lookups and app/result/backend access without
      changing descriptor behavior or forcing evaluation prematurely.
- [ ] Resolve the static result-handle contract and phantom generic; preserve
      serialization and extraction behavior.
- [ ] Type existing canvas composition and its actual supported FireX interface.
- [ ] Recount the full production inventory and proceed with remaining
      `firexkit`, application, and consumer cleanup in small batches.
- [ ] Revisit CI only after most existing debt is resolved.
- [ ] Later include tests and finish repository-wide strict cleanup.

Runtime argument/result validation and Pydantic/coercion changes are outside
this sequence. The behavior findings in section 9 are a separate backlog, not
automatic additions to a typing batch.

### Decisions to settle before implementation

1. Is the standard public result handle concrete, or is there a real payload
   type parameter to preserve? What external subscription compatibility exists?
2. Which operations are guaranteed on every FireX canvas, including composed
   native canvases? Which adaptation strategy preserves actual runtime behavior?
3. Which task-factory options are part of the supported FireX contract, and how
   are non-FireX bases and result-changing filters represented?
4. What are the configuration/backend capabilities in metadata-only, active,
   test, and shutdown modes, and how are their existing guarantees represented
   accurately in static types without adding runtime validation?
5. Which verified discrepancies should be corrected upstream versus through
   local, versioned typing compatibility support?

## 13. Static validation and acceptance criteria

### Type-level consumer checks

Use explicitly typed consumer parameters and exact type assertions, so an
unannotated caller or accidental `Any` cannot make the probe vacuously pass.
Check these examples with Pyrefly in strict mode; they are not runtime input
validation tests.

Cover:

- `@app.task`, `@app.task()`, configured decorators, bound tasks, and any custom
  bases or direct creation forms actually used by FireX. Historical Celery
  call-shape discrepancies are not a requirement to support unused forms.
- Invalid non-callables, invalid arity, and incorrect inference for supported
  factory forms.
- The selected `base=`/`filter=` policy and the parent override contract.
- Preservation of function signatures through metadata decorators.
- BOG-compatible partial signatures, composition, and enqueue APIs.
- Registry lookup, task/request app access, result construction, and backend
  capabilities with no consumer-side Celery casts or meaningless parameters.
- Accurate result extraction and lifecycle-dependent return types.
- Rejection of methods that the actual runtime object does not provide.

### Behavior-preservation safety checks, not runtime type checking

Run relevant existing behavioral tests and focused regressions when necessary
to show that a typing/compatibility change preserves imports, registration,
descriptors, argument forwarding, or result handling. This does not introduce a
runtime type-checking project or a new validation test matrix as an acceptance
gate for static typing.

Keep annotations needed by existing runtime introspection/Pydantic behavior
resolvable; do not expand validators, coercion, or annotation-driven enforcement.
New runtime type validation remains deferred, independently of static completion.

### Rules for a completed boundary batch

- The exported FireX contract matches the objects and behavior actually exposed.
- Any cast is narrow, local to the boundary, justified by verified runtime
  guarantees in existing behavior/source, and covered by Pyrefly consumer
  checks. Do not introduce runtime validators merely to justify annotations.
- No blanket ignores, baseline, `Any` replacement of the framework, or lowered
  severity is used to declare success.
- No legitimate stub-discovered runtime bug is merely silenced.
- The full production scan is rerun and newly exposed diagnostics are explained.
- Ordinary consumers do not gain Celery-specific typing workarounds.
- No CI or IDE work is introduced implicitly.
- Acceptance measures static diagnostics in the selected scope. Completing this
  migration does not require runtime type enforcement and must not be presented
  as proof that execution-time input/result validation exists.

## 14. Reproducing the diagnostic inventory

Run from the `firexapp` repository root, using its `.venv`. These are inventory
commands: a nonzero checker exit is expected while diagnostics remain.

For the already-installed checkout, install the existing `test` dependency
group without changing the lockfile, rebuilding the project, or removing
additional installed packages:

```bash
uv sync --group test --locked --inexact --no-install-project
```

This is the `test` dependency group, not the empty optional `test` extra.
The temporary local celery-types wheel must still exist at its configured path.

The default command now uses the checked-in production-only strict
configuration, including warnings; no temporary scope configuration is needed:

```bash
.venv/bin/pyrefly check
```

Keep diagnostic reports outside the repository:

```bash
mkdir -p "/tmp/.smx-$USER"
report_dir="$(mktemp -d "/tmp/.smx-$USER/firex-pyrefly.XXXXXX")"

.venv/bin/pyrefly check --count-errors=0 --summary=full \
    --output="json:$report_dir/production.json"

.venv/bin/pyrefly dump-config --max-files all \
    > "$report_dir/resolved-config.txt"
```

Do not compare the default production total with a historical full-repository
total as though excluding tests/docs fixed those diagnostics. Focused static
consumer checks can still pass explicit file arguments, including selected
tests, without changing the default milestone scope.

Track static contracts and counterexamples alongside diagnostic counts. The goal
is an accurate, statically checked FireX boundary, not merely a quieter checker
or a claim of runtime type validation.
