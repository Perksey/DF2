## Paths in DF2

It is expected that the use of paths in DF2 (similar in nature to Unix file paths, but not exactly the same) will be extensive, as will be concatention operations on them: `GetFullPath` and `GetRelativePath`. This document will provide a specification for how these operations are to behave as well as contain some considerations that will drive the design of concatenation functions.

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
- _Normalized paths_ have no _skip segments_ in them and always start with a _segment_ and end with a _separator_.

[Rationale: it is expected that most implementations will choose to normalize the paths as they enter the process, and work with normalized paths in memory due their superior performance characteristics. The restriction on the start and end of the normalized path are to ensure they always have the same number of _segments_ as they have _separators_, which simplifies the implementation of concatenation functions.]

- _Absolute paths_ are _normalized paths_ that have no _backtrack segments_.
- _Relative paths_ are _normalized paths_ that can only have _backtrack segments_ at the beginning of the _path_.

[Rationale: the concatenation functions operate on _relative_ and _absolute paths_. Defining them explicitly allows the implementation to not validate the implicit assumption these functions will have to make about their inputs. The restriction of _backtrack segments_ only being legal at the start of a _relative path_ is there to support the simplicity of implementation, both that of the validation and concatenation.]

## `GetFullPath` specification

TBD

## `GetRelativePath` specification

TBD

## Performance

When working on any algorithm, it is critical that data used in benchmarks reflects the real world. This is because performance of modern CPUs hinges critically on how predictable the input data is. It is thus essential to tune the algorithm to branch in predictable patterns.

As an illustration, this benchmark of a prototype implementation of `GetFullPath` illustrates the differences between predictable and unpredictable (data-dependent) control flow:
![image](https://user-images.githubusercontent.com/62474226/104724925-6e9a5c00-5742-11eb-963b-d19859d5465a.png)

Conceptually, the prototype copies all segments from the source paths into the result, looking for separators. Running it over data where there is a 50/50 chance the next `char` will be a separator will not be very performant. At the same time, fully branchless implementation would most certainly be slower in the `PureConcatNoSeparators` benchmark. As such it is critical to identify patterns that occur frequently in real-world paths and optimize for them, finding the "sweet spot" for branching.

In practice, we are most interested in the distribution of segment lengths. Obviously, there are no real-world systems that use DF2 paths today, and so we have to use surrogates, like file system paths. Running the following Powershell script shows that the assumption of the average segment being 10 characters long is mostly correct.
```Powershell
C:\Program Files> ls * -dir -r | % { $_.Name.Length } | where { $_ -gt 2 } | measure -all

Count             : 12504
Average           : 10.0473448496481
Sum               : 125632
Maximum           : 79
Minimum           : 3
StandardDeviation : 7.80063290882309

C:\Program Files (x86)> ls * -dir -r | % { $_.Name.Length } | where { $_ -gt 2 } | measure -all

Count             : 34843
Average           : 11.31960508567
Sum               : 394409
Maximum           : 89
Minimum           : 3
StandardDeviation : 9.20726908293111
```
We exclude paths that are less than 2 characters in length from the distribution as those are unlikely to be user-generated and mostly represent things like localization folders.

What is concerning about this data though is how big the standard deviation is. More data from more sources is clearly needed.