# What?

`armi` is an utility that makes a local mirror of Arch Linux packages.

# Why?

Arch Linux repository consists of two parts: the packages database and packages themself.
There are race conditions when packages are missing as files but still present in the database as records.
rsync is not the answer because it knows nothing about the database records.
So I created the `armi` to keep packages and their database in the guaranteed consistent state.
`armi` fetches the database, unpack it into memory and fetch packages according to database records.

# Usage

See `./armi -h` for the details.

# License

GPL.
