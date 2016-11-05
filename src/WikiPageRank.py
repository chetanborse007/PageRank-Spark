#!/usr/bin/python
'''
@File:           WikiPageRank.py
@Description:    PySpark application for counting the number of wikipedia pages
                 in corpus.
                 It consists both types of links,
                    a. Crawled links
 					b. Links that are not crawled yet
@Author:         Chetan Borse
@EMail:          cborse@uncc.edu
@Created on:     10/31/2016
@Usage:          spark-submit --master yarn --deploy-mode client 
                    src/WikiPageRank.py
                    -i "input/PageRank"
                    -o "output/Spark/PageRank/TopWikiPage"
                    -t 10
                    -d 0.85
                    -k 100
@python_version: 2.6
===============================================================================
'''


import argparse
import re
import warnings

from pyspark import SparkContext, SparkConf

warnings.filterwarnings("ignore")


dampingFactor = 0.85
corpusList = None


def ExtractTitleAndOutlinks(wikiPage):
    """
    Extract title and its outgoing links for a given wikipedia page.

    @param: wikiPage => Wikipedia page

    @yields:         => Tuple of (<Title>, <List of outgoing links>)
    """
    title = ''
    outlinks = []

    if not wikiPage:
        return

    # Patterns for extracting title and text body from wikipedia page
    titleMatcher = re.match(r'.*<title>(.*?)</title>.*', wikiPage, re.M)
    textMatcher = re.match(r'.*<text.*>(.*?)</text>.*', wikiPage, re.M)

    # Extract title of wikipedia page
    if titleMatcher:
        title = titleMatcher.group(1).strip()

    # Extract text body of wikipedia page
#    if textMatcher:
#        text = textMatcher.group(1).strip()

    # Extract outgoing links from wikipedia page
#    if text:
    outlinks = re.findall(r'\[\[(.*?)\]\]', wikiPage, re.DOTALL)
    outlinks = map(lambda x: x.strip(), outlinks)
    outlinks = filter(None, outlinks)

    # If a valid title exists, then yield;
    # (<Title>, <List of outgoing links>)
    if title:
        yield (title, outlinks)

        # Also, emit (<Outgoing link>, <[]>) for handling sink nodes
        for outlink in outlinks:
            yield (outlink, [])


def PageRankDistributor(wikiPage):
    """
    Distribute current page rank equally over outgoing links of 
    a given wikipedia page.

    @param: wikiPage => Wikipedia page

    @yields:         => Tuple of (<Outgoing link>, <Page rank contribution, []>) or
                                 (<Title>, <0.0, List of outgoing links>)
    """
    # If wikipedia page does not have any outgoing link,
    # then distribute page rank over all links in corpus.
    # Note: Only if handling for sink nodes is requested.
    if not wikiPage[1][1]:
#         allLinks = corpusList
        pass

    # Calculate page rank contribution
    if not wikiPage[1][1]:
#         pageRankContrib = wikiPage[1][0] / len(allLinks)
        pass
    else:
        pageRankContrib = wikiPage[1][0] / len(wikiPage[1][1])

    # Distribute page rank equally over outgoing links
    if not wikiPage[1][1]:
#         for outlink in allLinks:
#             yield (outlink, (pageRankContrib, []))
        pass
    else:
        for outlink in wikiPage[1][1]:
            yield (outlink, (pageRankContrib, []))

    # Also, yield (<Title>, <0.0, List of outgoing links>)
    yield (wikiPage[0], (0.0, wikiPage[1][1]))


def PageRankCalculator(wikiPage):
    """
    Calculate new page rank of a given wikipedia page.

    @param: wikiPage => Wikipedia page

    @yields:         => Tuple of (<Title>, <New Page rank, List of outgoing links>)
    """
    pageRank = (1.0 - dampingFactor) + dampingFactor * wikiPage[1][0]
    return (wikiPage[0], (pageRank, wikiPage[1][1]))


def PageRank(**args):
    """
    Entry point for Page Rank application, which computes page rank 
    over corpus of wikipedia page.
    """
    global dampingFactor;

    # Read arguments
    input = args['input']
    output = args['output']
    iterations = args['iterations']
    dampingFactor = args['damping_factor']
    resultSize = args['top_k']

    # Create SparkContext object
    conf = SparkConf()
    conf.setAppName("WikiPageRank")
    sc = SparkContext(conf=conf)

    # Read in the corpus of wikipedia pages
    input = sc.textFile(input)

    # Generate a link graph between wikipedia pages and its outgoing links
    wikiLinkGraph = input.flatMap(ExtractTitleAndOutlinks) \
                         .filter(lambda x: x != None) \
                         .reduceByKey(lambda x, y: x + y)

    # Count the number of wikipedia pages in corpus
    corpusList = wikiLinkGraph.keys()
    corpusSize = wikiLinkGraph.count()

    # Initialize page rank score for every wikipedia page in corpus
    initPageRank = 1.0 / corpusSize
    wikiPageRank = wikiLinkGraph.map(lambda x: (x[0], (initPageRank, x[1])))

    # Compute page rank of wikipedia pages using specified number of iterations
    for i in range(iterations):
        wikiPageRank = wikiPageRank.flatMap(PageRankDistributor) \
                                   .filter(lambda x: x != None) \
                                   .reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])) \
                                   .map(PageRankCalculator)

    # Retrieve top K wikipedia pages based on their page ranks
    topWikiPage = wikiPageRank.flatMap(lambda x: [(x[0], x[1][0])]) \
                              .sortBy(lambda x: x[1], ascending=False, numPartitions=1) \
                              .take(resultSize)

    # Save results
    topWikiPage = sc.parallelize(topWikiPage) \
                    .coalesce(1, shuffle=False) \
                    .map(lambda x: x[0] + '\t' + str(x[1]))
    topWikiPage.saveAsTextFile(output)

    # Shut down SparkContext
    sc.stop()


if __name__ == "__main__":
    """
    Entry point.
    """
    # Argument parser
    parser = argparse.ArgumentParser(description='Wiki Page Rank Application',
                                     prog='spark-submit --master yarn --deploy-mode client \
                                           src/WikiPageRank.py \
                                           -i <input> \
                                           -o <output> \
                                           -t <iterations> \
                                           -d <damping factor> \
                                           -k <top k result>')

    parser.add_argument("-i", "--input", type=str, required=True,
                        help="Input for Page Rank computations.")
    parser.add_argument("-o", "--output", type=str, required=True,
                        help="Output directory of top K results.")
    parser.add_argument("-t", "--iterations", type=int, default=10,
                        help="Number of iterations for Page Rank computations, default: 10")
    parser.add_argument("-d", "--damping_factor", type=float, default=0.85,
                        help="Damping factor, default: 0.85")
    parser.add_argument("-k", "--top_k", type=int, default=100,
                        help="Top K results to be retrieved, default: 100")

    # Read user inputs
    args = vars(parser.parse_args())

    # Run Wikipedia Page Rank Application
    PageRank(**args)

