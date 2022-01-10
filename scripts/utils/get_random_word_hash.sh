#!/bin/bash
# get random word hash like: apple-banana-carrot-tree

NUM_WORDS=${1:-4}

DICT_FILE_LOCAL=/tmp/clean_words.txt
DICT_FILE_REMOTE=https://github.com/InnovativeInventor/dict4schools/blob/master/safedict_full.txt?raw=true
DICT_FILE_HASH=8baa88ae4f04bcb394017397491853a4

# if dict_file_local does not exist or if it's md5sum doesn't match dict_file_hash, download dict_file_remote
if [ ! -f $DICT_FILE_LOCAL ] || [ $(md5sum $DICT_FILE_LOCAL | cut -d ' ' -f 1) != $DICT_FILE_HASH ]; then
    wget -O $DICT_FILE_LOCAL $DICT_FILE_REMOTE &> /dev/null
    if [ $? -ne 0 ]; then
        exit 1
    fi
fi

for i in $(seq 1 $NUM_WORDS); do
    # get random word from dict_file_local
    WORD=$(shuf -n 1 $DICT_FILE_LOCAL)
    
    # remove newline
    WORD=${WORD%$'\n'}
    
    # add word to output string
    if [ $i -eq 1 ]; then
        OUT_STR="$WORD"
    else
        OUT_STR="$OUT_STR-$WORD"
    fi
done

echo -e $OUT_STR