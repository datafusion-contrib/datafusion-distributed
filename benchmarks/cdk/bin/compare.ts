import {Command} from "commander";
import {BenchResult} from "./@results";

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

    const prevResults = BenchResult.loadMany(options.dataset, program.args[0])
    const newResults = BenchResult.loadMany(options.dataset, program.args[1])

    console.log(`=== Comparing results from engine '${program.args[0]}' [left] with '${program.args[1]}' [right] ===`);
    for (const result of newResults) {
        const prev = prevResults.find(v => v.id === result.id)
        if (!prev) {
            continue
        }
        result.compare(prev)
    }
}

main()
    .catch(err => {
        console.error(err)
        process.exit(1)
    })
