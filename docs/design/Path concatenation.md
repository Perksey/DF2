## Paths in DF2

It is expected that the use of paths in DF2 (similar in nature to Unix file paths, but not exactly the same) will be extensive, as will be concatention operations on them: `GetFullPath` and `GetRelativePath`. This document will provide a specification for how these operations are to behave as well as contain some considerations that will drive the design of concatenation functions.

## Definitions

Fundamentally, DF2 _paths_ are _strings_ that consist of _segments_ that are separated by _separators_. Two _paths_ are equal if all their _segments_ are equal. _Paths_ can have both _trailing_ and _leading_ _separators_.

_Strings_ are arrays of `char`s that are interpreted as UTF-16 encoded character sequences and have a trailing zero `char`. _Strings_ do not need to be valid UTF-16.

_Segments_ are _strings_ without the trailing zero-byte. _Segments_ cannot have _separators_ in them. There are special _segments_ which have specific meaning in DF2 paths:
- _Skip segment_ is a _segment_ 1 `char` in length, this `char` being a dot: `.`.
- _Backtrack segment_ is a _segment_ 2 `char`s in length, those being two dots: `..`.
- _Empty segment_ is a _segment_ 0 `char`s in length.

Two _segments_ are equal if all their `char`s are bitwise-equal. _Empty segments_ are only equal to each other.

_Separators_ are `char`s equal to the forward slash: `/`.

## `GetFullPath` specification

TBD

## `GetRelativePath` specification

TBD

## Performance

When working on any algorithm, it is critical that data used in benchmarks reflects real-world. This is because performance of modern CPUs hinges critically on how predictable the input data is. It is thus essential to tune the algorithm to branch in predictable patterns.

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
