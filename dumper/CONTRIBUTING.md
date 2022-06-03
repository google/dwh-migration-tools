# Contributing to Metadata and Log Dumper

## Dependencies

To checkout and compile the dumper the following tools and dependencies are needed (for non debian-based linux distributions the command may be different):
```
sudo apt-get install openjdk-11-jdk git thrift-compiler
```

## Checkout and compile

To checkout and compile the dumper use the following commands:

```
git clone https://github.com/google/dwh-migration-tools.git
cd dwh-migration-tools/
./gradlew :dumper:app:installDist
```

The dumper can then be run with:
```
dumper/app/build/install/app/bin/dwh-migration-dumper --help
```

## Binary vs Source Release

Any change to the gradle files, or files layout should take into account that a requirement is to have the `bin/dwh-migration-dumper` command behave identically in both source and binary releases.
So in a binary release the command will just invoke the dumper binary, in a source release the command will compile the dumper and then invoke it using its build location.
