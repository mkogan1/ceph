#include <iostream>
#include <fstream>
#include <string>
#include <bits/stdc++.h>

using namespace std;

// Code for checking differences line by line in rgwKeys.txt and metaKeys.txt

int main () {
    ifstream rgwKeys, metaKeys;
    string line1, line2;
    int lineNum = 0;
    bool match = true;

    rgwKeys.open ("rgwKeys.txt");
    metaKeys.open ("metaKeys.txt");

    if (rgwKeys.is_open() && metaKeys.is_open()) {

        rgwKeys >> line1;
        if(line1.substr(line1.length()-3) != "dat") {
            cout << "Fail: Object file did not get saved into meta data. First line of .dat is missing." << endl;
            exit(0);
        }
        // int size = stoi(line1.substr(line1.find("_")+1));

        // Checks for EOF of either rgwKeys.txt or metaKeys.txt
        // because we know rgwKeys.txt will have the .dat file at the end,
        // which we will compare to the object name (TODO)
        while (!rgwKeys.eof() && !metaKeys.eof()) {
            lineNum++;

            rgwKeys >> line1;
            metaKeys >> line2;

            if (line1 != line2) {
                cout << "Fail: The keys do not match on line " << lineNum << "." << endl;
                match = false;
                break;
            }
        }

        if (match)
            cout << "Success: The keys match." << endl;
        // Checking the object size matches the number of blocks 
        // if (ceil(size/(double)4) == numOfBlocks) 
        //     cout << "Success! Object size of " << size << " makes " << numOfBlocks << " blocks." << endl;
        // else cout << "Fail! Object size does not match number of blocks" << endl;

        rgwKeys.close();
        metaKeys.close();
    }
    else
        cout << "Fail: Unable to open files." << endl;

    return 0;
}