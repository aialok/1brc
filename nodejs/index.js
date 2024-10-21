import * as readline from 'node:readline';
import * as fs from 'node:fs';

const fileName = 'data/measurements.txt';
const stream = fs.createReadStream(fileName);
const lineStream = readline.createInterface({ input: stream });

const measurements = new Map();
let processedLines = 0;
const BATCH_SIZE = 1000000;  // Process in batches to let Node.js event loop free up

async function processBatch(batch) {
  for (const line of batch) {
    const [stationNameStream, temperatureStream] = line.split(';');
    
    // Basic validation in case of bad input
    if (!stationNameStream || !temperatureStream) continue;

    const stationName = stationNameStream.trim();
    const temperature = Math.floor(parseFloat(temperatureStream.trim()));

    if (!measurements.has(stationName)) {
      measurements.set(stationName, {
        temperatureSum: temperature,
        temperatureCount: 1,
        min: temperature,
        max: temperature
      });
    } else {
      const current = measurements.get(stationName);
      current.temperatureSum += temperature;
      current.temperatureCount++;
      current.min = Math.min(current.min, temperature);
      current.max = Math.max(current.max, temperature);
    }
  }
}

async function processFile() {
  const buffer = [];

  for await (const line of lineStream) {
    buffer.push(line);
    processedLines++;

    // Process in batches to avoid blocking
    if (buffer.length === BATCH_SIZE) {
      await processBatch(buffer);
      buffer.length = 0; // Clear buffer

      // Optional: Manual garbage collection to release memory if needed
      if (global.gc) global.gc();

      // Log progress every batch
      if (processedLines % BATCH_SIZE === 0) {
        console.log(`Processed ${processedLines} lines...`);
      }
    }
  }

  // Process the remaining lines after the last batch
  if (buffer.length > 0) {
    await processBatch(buffer);
  }

  console.log('File processing complete.');
  console.log(`Total processed lines: ${processedLines}`);
  console.log(measurements);
}

processFile().catch((err) => {
  console.error('Error processing file:', err);
});
