import express from 'express';
import fs from 'node:fs';
import path from 'node:path';
import { createServer as createViteServer } from 'vite';
import { runPipeline, type PipelineInput } from './src/pipeline.ts';
import { SAMPLE_DATASET } from './src/sampleData.ts';

const PORT = Number(process.env.PORT || 3000);
const HOST = '0.0.0.0';
const DATA_DIR = process.env.DATA_DIR || '/data';

function readCsvDir(dirPath: string): { name: string; content: string }[] {
  if (!fs.existsSync(dirPath)) return [];
  const entries = fs.readdirSync(dirPath, { withFileTypes: true });
  return entries
    .filter((e) => e.isFile() && e.name.endsWith('.csv'))
    .sort((a, b) => a.name.localeCompare(b.name))
    .map((e) => ({
      name: e.name,
      content: fs.readFileSync(path.join(dirPath, e.name), 'utf-8'),
    }));
}

function loadFromDataDir(dir: string): PipelineInput {
  const manifestPath = path.join(dir, 'manifest.json');
  const productsPath = path.join(dir, 'products.csv');
  const campaignsPath = path.join(dir, 'campaigns.csv');
  if (!fs.existsSync(manifestPath)) {
    throw new Error(`manifest.json not found in DATA_DIR (${dir})`);
  }
  return {
    manifest: fs.readFileSync(manifestPath, 'utf-8'),
    productsCsv: fs.existsSync(productsPath) ? fs.readFileSync(productsPath, 'utf-8') : 'product_id,category\n',
    campaignsCsv: fs.existsSync(campaignsPath) ? fs.readFileSync(campaignsPath, 'utf-8') : 'campaign_id,channel\n',
    eventsCsvs: readCsvDir(path.join(dir, 'events')),
    clicksCsvs: readCsvDir(path.join(dir, 'clicks')),
    linksCsvs: readCsvDir(path.join(dir, 'links')),
  };
}

async function startServer() {
  const app = express();
  app.use(express.json({ limit: '50mb' }));

  app.get('/api/health', (_req, res) => {
    res.json({ ok: true, runtime: 'node', version: process.version });
  });

  app.get('/api/data-dir-status', (_req, res) => {
    const exists = fs.existsSync(path.join(DATA_DIR, 'manifest.json'));
    res.json({ dataDir: DATA_DIR, hasManifest: exists });
  });

  app.get('/api/sample', (_req, res) => {
    try {
      const result = runPipeline(SAMPLE_DATASET);
      res.json({ input: SAMPLE_DATASET, result });
    } catch (err) {
      res.status(500).json({ error: err instanceof Error ? err.message : 'Failed to run sample pipeline' });
    }
  });

  app.post('/api/run', (req, res) => {
    try {
      const body = req.body as { useDiskDataDir?: boolean } & Partial<PipelineInput>;
      const input: PipelineInput = body.useDiskDataDir
        ? loadFromDataDir(DATA_DIR)
        : {
            manifest: body.manifest ?? SAMPLE_DATASET.manifest,
            productsCsv: body.productsCsv ?? SAMPLE_DATASET.productsCsv,
            campaignsCsv: body.campaignsCsv ?? SAMPLE_DATASET.campaignsCsv,
            eventsCsvs: body.eventsCsvs ?? SAMPLE_DATASET.eventsCsvs,
            clicksCsvs: body.clicksCsvs ?? SAMPLE_DATASET.clicksCsvs,
            linksCsvs: body.linksCsvs ?? SAMPLE_DATASET.linksCsvs,
          };
      const result = runPipeline(input);
      res.json({ result });
    } catch (err) {
      res.status(400).json({ error: err instanceof Error ? err.message : 'Pipeline execution error' });
    }
  });

  if (process.env.NODE_ENV !== 'production') {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  } else {
    const distDir = path.resolve(process.cwd(), 'dist');
    app.use(express.static(distDir));
    app.get('/{*splat}', (_req, res) => {
      res.sendFile(path.join(distDir, 'index.html'));
    });
  }

  app.listen(PORT, HOST, () => {
    console.log(`Shopping Sessions server listening on http://${HOST}:${PORT}`);
  });
}

startServer().catch((err) => {
  console.error('Server startup failed:', err);
  process.exit(1);
});
