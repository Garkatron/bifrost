default:
    @just --list --unsorted

root := justfile_directory()

[group('Client')]
client:
    odin run {{root}}/tests/client -sanitize:address

[group('Server')]
server:
    odin run {{root}}/tests/server -sanitize:address
