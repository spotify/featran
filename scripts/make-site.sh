#!/bin/bash

SOCCO=true sbt ++2.12.7 examples/clean examples/compile
sbt ghpagesPushSite
