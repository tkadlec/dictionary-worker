#!/bin/bash
rm ../assets/*
hash=$(openssl dgst -sha256 -binary dictionary.dat | base64 | tr -d '\n' | tr '+/' '-_' | tr -d '=')
cp dictionary.dat ../assets/$hash.dat
