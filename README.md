# Kron

[![Build Status][build-badge]][builds]
[![NuGet][nuget-badge]][nuget-pkg]
[![MyGet][myget-badge]][edge-pkgs]

Kron is a [.NET Standard][netstd] library that implements a simple and generic
job scheduler. It is simple because it only provides building blocks. It is
generic because it does not care about how jobs and schedules are defined as
long as jobs are `Task`-based functions and schedules are functions of time.


[netstd]: https://docs.microsoft.com/en-us/dotnet/articles/standard/library
[build-badge]: https://img.shields.io/appveyor/ci/raboof/kron.svg
[myget-badge]: https://img.shields.io/myget/raboof/v/Kron.svg?label=myget
[edge-pkgs]: https://www.myget.org/feed/raboof/package/nuget/Kron
[nuget-badge]: https://img.shields.io/nuget/v/Kron.svg
[nuget-pkg]: https://www.nuget.org/packages/Kron
[builds]: https://ci.appveyor.com/project/raboof/kron
