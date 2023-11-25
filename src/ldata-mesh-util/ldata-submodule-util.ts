import * as fs from 'fs';

export function getModifiedDate(datasetName: string): number {
    try {
        let dateStr: string = fs
            .readFileSync(`./${datasetName}-date.log`, 'utf-8')
            .split('\n')[0]
            .split('\t')[0];

        let ret: number = 0;

        if (dateStr) {
            ret = Number.parseInt(dateStr, 0);
            if (isNaN(ret)) {
                ret = 0;
            }
        }

        return ret;
    } catch (error) {
        return -1;
    }
}

export function getModifiedRootFolders(datasetName: string) {
    try {
        // Get the list of modified files in the last commit of the submodule
        const output = fs.readFileSync(`./${datasetName}-diffs.log`, 'utf-8');

        // Split the output into lines (file paths)
        const files = output.split('\n');

        // Extract root folders from the file paths and remove duplicates
        const rootFolders = new Set<string>();
        files.forEach((file) => {
            const rootFolder = file.split('/')[0];
            if (rootFolder) {
                rootFolders.add(rootFolder);
            }
        });

        return Array.from(rootFolders);
    } catch (error) {
        console.error('Error fetching modified folders:', error);
        console.log('Returning empty modifications list');
        return [];
    }
}
