#!/bin/zsh
for file in ./Log-???????????????.log; do
    if grep -q "column_family_set_" "$file"; then
        # if grep -q "local_sv_" "$file"; then
        echo "$file"
        # fi
    fi
done
