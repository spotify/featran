#!/bin/bash

SOCCO=true sbt ++2.12.8 examples/clean examples/compile
sbt ghpagesPushSite
