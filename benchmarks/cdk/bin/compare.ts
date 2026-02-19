import {Command} from "commander";
import { BenchmarkRun, BenchResult } from "./@results";

async function main() {
    const program = new Command();

    program
        .requiredOption('--dataset <string>', 'Dataset to run queries on')
        .argument("<base_engine>", "the base engine")
        .argument("<compare_engine>", "the engine to compare to")
        .parse(process.argv);

    const options = program.opts();
    if (program.args.length != 2) {
        throw new Error(`Expected exactly 2 arguments, got ${program.args.length}`)
    }

    const prevRun = new BenchmarkRun(options.dataset, program.args[0])
    prevRun.loadResults()
    const newRun = new BenchmarkRun(options.dataset, program.args[1])
    newRun.loadResults()

    newRun.compare(prevRun)
}

main()
    .catch(err => {
        console.error(err)
        process.exit(1)
    })
