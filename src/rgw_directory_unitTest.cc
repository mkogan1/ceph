#include <iostream>
#include <fstream>
#include <string>
#include <bits/stdc++.h>

using namespace std;

// Code for checking differences line by line in rgwKeys.txt and metaKeys.txt

int main () {
    ifstream rgwKeys, metaKeys;
    string line1, line2;

    rgwKeys.open ("rgwKeys.txt");
    metaKeys.open ("metaKeys.txt");

    int numOfBlocks = 1;
    bool match = true;

    if (rgwKeys.is_open() && metaKeys.is_open()) {

        /* Check to see if first line has .dat format to save the file size */
        rgwKeys >> line1;
        if(line1.substr(line1.length()-3) != "dat") {
            cout << "Fail: Object file did not get saved into meta data. First line of .dat is missing." << endl;
            exit(1);
        }
        int size = stoi(line1.substr(line1.find("_")+1));
        
        /* Check each line in metaKeys and rgwKeys to see if they match. Both files were sorted in order. */
        while (rgwKeys >> line1 && metaKeys >> line2) {
            if (line1 != line2) {
                cout << "Fail: The keys do not match on line " << numOfBlocks << "." << endl;
                match = false;
                exit(1);
            }
            numOfBlocks++;
        }

        if (match)
            cout << "Success: The keys match." << endl;
    
       /* Check the object size if it matches the number of blocks */
        if (ceil(size/(double)4) == numOfBlocks) 
            cout << "Success! Object size of " << size << " makes " << numOfBlocks << " blocks." << endl;
        else cout << "Fail! Object size does not match number of blocks" << endl;

        rgwKeys.close();
        metaKeys.close();
    }
    else
        cout << "Fail: Unable to open files." << endl;
        exit(2);

    return 0;
}