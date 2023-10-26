using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Queries;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Util;
using MarketingCodingAssignment.Models;
using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using System.Text;

namespace MarketingCodingAssignment.Services
{
    public class SearchEngine
    {
        // The code below is roughly based on sample code from: https://lucenenet.apache.org/

        private const LuceneVersion AppLuceneVersion = LuceneVersion.LUCENE_48;

        public SearchEngine()
        {

        }

        public List<FilmCsvRecord> ReadFilmsFromCsv()
        {
            List<FilmCsvRecord> records = new();
            string filePath = $"{System.IO.Directory.GetCurrentDirectory()}{@"/wwwroot/csv"}" + "//" + "FilmsInfo.csv";
            using (StreamReader reader = new(filePath))
            using (CsvReader csv = new(reader, new CsvConfiguration(CultureInfo.InvariantCulture)))
            {
                records = csv.GetRecords<FilmCsvRecord>().ToList();

            }
            using (StreamReader r = new(filePath))
            {
                string csvFileText = r.ReadToEnd();
            }
            return records;
        }

        // Read the data from the csv and feed it into the lucene index
        public void PopulateIndexFromCsv()
        {
            // Get the list of films from the csv file
            var csvFilms = ReadFilmsFromCsv();

            // Convert to Lucene format
            List<FilmLuceneRecord> luceneFilms = csvFilms.Select(x => new FilmLuceneRecord
            {
                Id = x.Id,
                Title = x.Title,
                Overview = x.Overview,
                Runtime = int.TryParse(x.Runtime, out int parsedRuntime) ? parsedRuntime : 0,
                Tagline = x.Tagline,
                Revenue = long.TryParse(x.Revenue, out long parsedRevenue) ? parsedRevenue : 0,
                VoteAverage = double.TryParse(x.VoteAverage, out double parsedVoteAverage) ? parsedVoteAverage : 0,
                ReleaseDate = DateTime.TryParse(x.ReleaseDate, out DateTime parsedReleaseDate) ? parsedReleaseDate : DateTime.MinValue
            }).ToList();

            // Write the records to the lucene index
            PopulateIndex(luceneFilms);

            return;
        }

        public void PopulateIndex(List<FilmLuceneRecord> films)
        {
            // Construct a machine-independent path for the index
            //string executablePath = System.Reflection.Assembly.GetEntryAssembly().Location;
            string basePath = $"{System.IO.Directory.GetCurrentDirectory()}";

            //string basePath = Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData);
            string indexPath = Path.Combine(basePath, "index");
            using FSDirectory dir = FSDirectory.Open(indexPath);

            // Create an analyzer to process the text
            StandardAnalyzer analyzer = new(AppLuceneVersion);

            // Create an index writer
            IndexWriterConfig indexConfig = new(AppLuceneVersion, analyzer);
            using IndexWriter writer = new(dir, indexConfig);

            //Add to the index
            foreach (var film in films)
            {
                Document doc = new()
                {
                    new StringField("Id", film.Id, Field.Store.YES),
                    new TextField("Title", film.Title, Field.Store.YES),
                    new TextField("Overview", film.Overview, Field.Store.YES),
                    new Int32Field("Runtime", film.Runtime, Field.Store.YES),
                    new TextField("Tagline", film.Tagline, Field.Store.YES),
                    new Int64Field("Revenue", film.Revenue ?? 0, Field.Store.YES),
                    new DoubleField("VoteAverage", film.VoteAverage ?? 0.0, Field.Store.YES),
                    new TextField("CombinedText", film.Title + film.Tagline + film.Overview, Field.Store.NO),
                    new TextField("ReleaseDate", film.ReleaseDate?.ToString("yyy-MM-dd",CultureInfo.InvariantCulture) ?? string.Empty, Field.Store.YES)


                };
                writer.AddDocument(doc);
            }

            writer.Flush(triggerMerge: false, applyAllDeletes: false);
            writer.Commit();

            return;
        }

        public void DeleteIndex()
        {
            // Delete everything from the index
            string basePath = $"{System.IO.Directory.GetCurrentDirectory()}";

            //string basePath = Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData);
            string indexPath = Path.Combine(basePath, "index");
            using FSDirectory dir = FSDirectory.Open(indexPath);
            StandardAnalyzer analyzer = new(AppLuceneVersion);
            IndexWriterConfig indexConfig = new(AppLuceneVersion, analyzer);
            using IndexWriter writer = new(dir, indexConfig);
            writer.DeleteAll();
            writer.Commit();
            return;
        }

        public SearchResultsViewModel Search(string searchString, int startPage, int rowsPerPage, int? durationMinimum, int? durationMaximum, double? voteAverageMinimum, int startDate, int endDate)
        {
            // Construct a machine-independent path for the index
            //string executablePath = System.Reflection.Assembly.GetEntryAssembly().Location;
            //string applicationDirectory = Path.GetDirectoryName(executablePath);

            string basePath = $"{System.IO.Directory.GetCurrentDirectory()}";
            //string basePath = "/Users/vivektekade/development";
            //string basePath = Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData);
            string indexPath = Path.Combine(basePath, "index");
            using FSDirectory dir = FSDirectory.Open(indexPath);

            // Create an analyzer to process the text
            StandardAnalyzer analyzer = new(AppLuceneVersion);

            // Create an index writer
            IndexWriterConfig indexConfig = new(AppLuceneVersion, analyzer);
            using IndexWriter writer = new(dir, indexConfig);
            using DirectoryReader reader = writer.GetReader(applyAllDeletes: true);
            IndexSearcher searcher = new(reader);
            int hitsLimit = 1000;
            TopScoreDocCollector collector = TopScoreDocCollector.Create(hitsLimit, true);

            // If there's no search string, just return everything.
            Query pq = new PhraseQuery()
                {
                    new Term("CombinedText", searchString.ToLowerInvariant())
                };
            Query rq = NumericRangeQuery.NewInt32Range("Runtime", durationMinimum, durationMaximum, true, true);

            //films with voting average less than voting average minimum value


            // Apply the filters.
            BooleanQuery bq = new()
            {
                { pq, Occur.MUST },
                { rq, Occur.MUST }
            };

            if (voteAverageMinimum.HasValue && voteAverageMinimum > 0)
            {
                Query vaq = NumericRangeQuery.NewDoubleRange("VoteAverage", voteAverageMinimum, 10, true, true);
                bq.Add(vaq, Occur.MUST);
            }

            //static string Serialize(DateTime? dateTime)
            //{
            //    return dateTime.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture);
            //}


            //bq.Add(dr, Occur.MUST);
            var newStartDate = new DateTime(startDate,1,1);
            //DateTime nstartDate = DateTime.ParseExact(startDate, "MM-dd-yyyy", CultureInfo.InvariantCulture);


            var newEndDate = DateTime.Today;
            
            //var ssd = Serialize(startDate);

            BytesRef startBytes = new BytesRef(Encoding.UTF8.GetBytes(startDate.ToString()));
            BytesRef endBytes = new BytesRef(Encoding.UTF8.GetBytes(endDate.ToString()));
            Query dr = new TermRangeQuery("ReleaseDate", startBytes, endBytes, true, true);
            bq.Add(dr, Occur.MUST);






            searcher.Search(bq, collector);
            int startIndex = (startPage) * rowsPerPage;
            TopDocs hits = collector.GetTopDocs(startIndex, rowsPerPage);
            ScoreDoc[] scoreDocs = hits.ScoreDocs;

            List<FilmLuceneRecord> films = new();
            foreach (ScoreDoc? hit in scoreDocs)
            {
                Document foundDoc = searcher.Doc(hit.Doc);
                FilmLuceneRecord film = new()
                {
                    Id = foundDoc.Get("Id").ToString(),
                    Title = foundDoc.Get("Title").ToString(),
                    Overview = foundDoc.Get("Overview").ToString(),
                    Runtime = int.TryParse(foundDoc.Get("Runtime"), out int parsedRuntime) ? parsedRuntime : 0,
                    Tagline = foundDoc.Get("Tagline").ToString(),
                    Revenue = long.TryParse(foundDoc.Get("Revenue"), out long parsedRevenue) ? parsedRevenue : 0,
                    VoteAverage = double.TryParse(foundDoc.Get("VoteAverage"), out double parsedVoteAverage) ? parsedVoteAverage : 0.0,
                    Score = hit.Score,
                    ReleaseDate = DateTime.TryParse(foundDoc.Get("ReleaseDate"), out DateTime parsedReleaseDate) ? parsedReleaseDate : null

                };
                films.Add(film);
            }

            


            SearchResultsViewModel searchResults = new()
            {
                RecordsCount = hits.TotalHits,
                Films = films.ToList()
            };

            return searchResults;
        }

        public void autocom() {

            string basePath = $"{System.IO.Directory.GetCurrentDirectory()}";
            //string basePath = "/Users/vivektekade/development";
            //string basePath = Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData);
            string indexPath = Path.Combine(basePath, "index");
            using FSDirectory dir = FSDirectory.Open(indexPath);

            // Create an analyzer to process the text
            StandardAnalyzer analyzer = new(AppLuceneVersion);

            // Create an index writer
            IndexWriterConfig indexConfig = new(AppLuceneVersion, analyzer);
            using IndexWriter writer = new(dir, indexConfig);
            using DirectoryReader reader = writer.GetReader(applyAllDeletes: true);
            IndexSearcher searcher = new(reader);
            int hitsLimit = 1000;
            TopScoreDocCollector collector = TopScoreDocCollector.Create(hitsLimit, true);




        }

    }


}

