## Log
The Log consists of:
* Segments
    * Store
        * A file that stores the `Records`
        * each "entry" in the store is a `[]byte` that looks like: `[ width | Record ]`, where `width` tells us many bytes the `Record` takes up.
    * Index
        * A file that is memory-mapped to a byte slice
        * Each entry has:
            * The record's offset relative to the base offset of the segment the index belongs to (4B for space efficiency)
            * The corresponding record's position in the store.