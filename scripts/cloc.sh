#!/bin/bash

cloc --include-lang=Go --exclude-dir="main,txnpaxos" .
