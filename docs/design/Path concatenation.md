## Paths in DF2

It is expected that the use of paths in DF2 (similar in nature to Unix file paths, but not exactly the same) will be extensive, as will be concatention operations on them: `GetFullPath` and `GetRelativePath`. This document will provide a specification for how these operations are to behave.

## Definitions

_Char_ is a Unicode code point. There are special _chars_ which have specific meaning in DF2 paths:
- _Separator_ is a _char_ corresponding to the code point `U+002F` - `Slash`.
- _Dot_ is a _char_ corresponding to the code point `U+002E` - `Full stop`.

Two _chars_ are equal if they refer to the same code point.

[Rationale: to support the performance of concatenation functions, it is neccessary to impose the restriction that equality comparisons be strictly ordinal.]

_Segments_ are sequences of _chars_ that have no _separators_ in them. There are special _segments_ which have specific meaning in DF2 paths:
- _Skip segment_ is a _segment_ that consists of one _dot_ or has no _chars_ at all.
- _Backtrack segment_ is a _segment_ that consists of two _dots_.

Two _segments_ are equal if all their _chars_ are equal, except for _skip segments_, which are always equal to each other.

DF2 _paths_ are sequences of _segments_ and _separators_. It is illegal for two _segments_ to follow each other. Two _separators_ that have no _chars_ between them are treated as a _separator_-_skip segment_-_separator_ triplet.

Three types of _paths_ represent subsets of the above definition:
- _Normalized paths_ have no _skip segments_ in them and always have the same number of _separators_ as _segments_. _Normalized paths_ that have at least one _segment_ always start with a _separator_ and end with a _segment_.

[Rationale: it is expected that most implementations will choose to normalize the paths as they enter the process, and work with normalized paths in memory due their superior performance characteristics. The restriction on the start and end of the normalized path are to ensure they always have the same number of _segments_ as they have _separators_, which simplifies the implementation of concatenation functions.]

- _Absolute paths_ are _normalized paths_ that have no _backtrack segments_.
- _Relative paths_ are _normalized paths_ that can only have _backtrack segments_ at the beginning of the _path_.

[Rationale: the concatenation functions operate on _relative_ and _absolute paths_. Defining them explicitly allows the implementation to not validate the implicit assumption these functions will have to make about their inputs. The restriction of _backtrack segments_ only being legal at the start of a _relative path_ is there to support the simplicity of implementation, both that of the validation and concatenation.]

## `Normalize` specification

`Normalize` is a function with one parameter: `path`, which is a _path_. The result of this function is a _normalized path_. Calling this function for a _path_ is to be called _normalization_, which is to be equivalent to the following algorithm:

1. If the _path_ does not start with a _separator_, prepend one.
2. If the _path_ ends with a _separator_, delete it.
3. For each _skip segment_ in the `path`, delete it and the _separator_ preceding it.
4. Delete all _separator_-_segment_ that is not a _backtrack segment_-_separator_-_backtrack segment_ quadruplets in the `path`.
5. Repeat step 4 until no such quadruplets are left.
6. Return the result of the above modifications to `path`.

## `GetFullPath` specification

`GetFullPath` is a function with two parameters: `appendPath`, which is a _relative path_, and `basePath`, which is an _absolute path_. The result of this funtion is an _absolute path_ or an error, whichever is determined by the an algorithm equivalent to the following:

1. Count the number of _backtrack segments_ in `appendPath`.
2. Delete as many _segments_ in `basePath`, starting from the end. If the operation cannot be performed due to `basePath` not having enough _segments_, return with error.
3. _Normalize_ `basePath`.
4. Delete all _backtrack segments_ from `appendPath`.
5. _Normalize_ `appendPath`.
6. Return the result as the sequence of all _chars_ in the `basePath` followed by all _chars_ in the `appendPath`.

## `GetRelativePath` specification

`GetRelativePath` is a function with two parameters: `path`, which is an _absolute path_, and `basePath`, which is also an _absolutePath_. The result of this function is a _relative path_, determined by an algorithm equivalent to the following:

1. If the _segments_ at the start of `path` and `basePath` are equal, delete them from both with the preceding separators.
2. Repeat step 1 until the _segements_ at the start of each path are no longer equal.
3. Count the number of segments in `basePath`.
4. Prepend that number of _separators_ and _backtrack segments_, alternating between the two and starting with a _separator_, to `path`.
5. Return the result of the above modifications to `path`.
