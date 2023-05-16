#include <iostream>
#include <string>

#include "src/client/dfs_client.h"

int main() {
    std::string command;
    while (true) {
        std::cout << "DFS >> ";
        std::getline(std::cin, command);

        if (command == "quit") {
            std::cout << "Exiting...\n";
            break;
        } else if (command == "hi") {
            std::cout << "hi...\n";
        }
    }

    return 0;
}