#!/usr/bin/env python

"""incomplete_submissions.py: Outputs a list of EGAS accessions and relevant information  for studies that have been published but not released."""

__author__ = "Mauricio Moldes"
__version__ = "0.1"
__maintainer__ = "Mauricio Moldes"
__email__ = "mauricio.moldes@crg.eu"
__status__ = "test"

import psycopg2
import config
import logging
import os
import sys

logger = logging.getLogger('incomplete_submissions')

""" CONNECTION TO DB """


def connection_plsql():
    conn_string = "host='" + str(config.plsql['host']) + "' dbname='" + str(
        config.plsql['dbname']) + "' user='" + str(
        config.plsql['user']) + "' password='" + str(config.plsql['password']) + "' port = '" + str(
        config.plsql['port']) + "'"
    conn_plsql = psycopg2.connect(conn_string)
    return conn_plsql


""" GET ALL THE PUBLISHED STUDIES ACCESSIONS """


def get_published_accession_accession(bibliography_connection):
    cursor = bibliography_connection.cursor()
    cursor.execute("select distinct ega_accession_id from bibliography.ega_study_article;")
    records = cursor.fetchall()
    return records


""" GET STUDY DATASETS """


def get_study_datasets(bibliography_connection, study):
    cursor = bibliography_connection.cursor()
    cursor.execute("select dt.ega_stable_id,"
                   "    st.submitter_box,"
                   "    count(distinct(dt.ega_stable_id)) as datasets"
                   "    from study_table st"
                   "    INNER JOIN study_dataset_table stt ON st.\"id\"= stt.study_id"
                   "    INNER JOIN dataset_table dt ON stt.dataset_id = dt.id"
                   "    where st.ega_stable_id = '" + study + "'"
                                                              "    group by dt.ega_stable_id, st.submitter_box;")
    records = cursor.fetchall()
    return records


""" GETS ALL NON RELEASED STUDIES ACCESSIONS """


def get_unreleased_study_accession(conn_plsql):
    cursor = conn_plsql.cursor()
    cursor.execute("select ega_stable_id from study_table where final_release_status = 'NOT_RELEASED';")
    records = cursor.fetchall()
    return records


""" COMPARES UNRELEASED STUDIES WITH PUBLISHED STUDIES """


def compare_studies(published_accession_ID, unreleased_study_ID):
    published_studies_non_released = list()
    for unreleased_study in unreleased_study_ID:
        if unreleased_study in published_accession_ID:
            published_studies_non_released.append(unreleased_study)
    return published_studies_non_released


def count_runs(conn_plsql, accession):
    cursor = conn_plsql.cursor()
    cursor.execute(" select st.ega_stable_id, count(rt.id), string_agg(rt.ega_stable_id, ',') as list_of_runs"
                   " from study_table st"
                   " left join experiment_table et on st.id = et.study_id and et.disabled_flag = false"
                   " left join run_table rt on et.id = rt.experiment_id and rt.disabled_flag = false"
                   " where st.ega_stable_id in ('" + accession + "')"
                                                                 " and st.disabled_flag = false"
                                                                 " group by st.ega_stable_id"
                                                                 " order by 1 asc")
    records = cursor.fetchall()
    return records


def count_analysis(conn_plsql, accession):
    cursor = conn_plsql.cursor()
    cursor.execute(" select st.ega_stable_id, count(ant.id), string_agg(ant.ega_stable_id, ',') as list_of_analyses"
                   " from study_table st"
                   " left join analysis_table ant on st.id = ant.study_id and ant.disabled_flag = false"
                   " where st.ega_stable_id in ('" + accession + "')"
                                                                 " and st.disabled_flag = false"
                                                                 " group by st.ega_stable_id"
                                                                 " order by 1 asc")
    records = cursor.fetchall()
    return records


""" OUTPUTS TO CONSOLE """


def output_incomplete_studies_to_console(record,runs,analysis, dataset):
    if not dataset:
        print(str(record[4]) + " | " + str(record[0]) + " | " + str(record[1]) + " | " + str(record[3]) + " | " + str(
            record[2]) + " | 0 " + " | " + str(runs[0][1]) + " | " + str(analysis[0][1]))
    else:
        tmp_dataset_id = list()
        for dataset_id in dataset:
            tmp_dataset_id.append(dataset_id[0])
            dataset_str =  (','.join(tmp_dataset_id))
        print(str(record[4]) + " | " + str(record[0]) + " | " + str(record[1]) + " | " + str(record[3]) + " | " + str(
            record[2]) + " | " + str(len(dataset)) + " | " + str(runs[0][1]) + " | " + str(analysis[0][1])+ " | " + str(dataset[0][1]) + " | " + dataset_str )


""" QUERY FURTHER INFORMATION ABOUT THE STUDY AND OUTPTUS TO CONSOLE"""


def query_output_incomplete_studies(conn_plsql, published_studies_non_released):
    print("The number of published studies whose accessions are not live in the EGA is : " + str(
        len(published_studies_non_released)))
    print ("ega_accession | europubmed | article title | journal title | publish data |  ega-box | count runs |  count analysis | dataset id ")
    for published_non_released in published_studies_non_released:
        cursor = conn_plsql.cursor()
        sql = (" SELECT article.article_id,"
               " article.title,"
               " article.first_publication_date,"
               " article.journal_title,"
               " ega_study_article.ega_accession_id"
               " FROM bibliography.article"
               " INNER JOIN bibliography.ega_study_article ON ega_study_article.article_id = article.article_id"
               " WHERE ega_study_article.ega_accession_id =  %s "
               " ORDER BY first_publication_date ASC")
        accession = str(published_non_released[0])
        cursor.execute(sql, [accession])
        record = cursor.fetchone()
        dataset = get_study_datasets(conn_plsql, accession)
        runs = count_runs(conn_plsql, accession)
        analysis = count_analysis(conn_plsql, accession)
        output_incomplete_studies_to_console(record,runs,analysis, dataset)


"""" MAIN INCOMPLETE SUBMISSIONS """


def incomplete_submissions():
    logger.info("Process started")
    try:
        # opens connection
        conn_plsql = connection_plsql()
        if conn_plsql:
            # get unreleased sudy ids
            unreleased_study_accession_ = get_unreleased_study_accession(conn_plsql)
            # get published study ids
            published_accession_acession = get_published_accession_accession(conn_plsql)
            # compare studies
            published_studies_non_released = compare_studies(published_accession_acession, unreleased_study_accession_)
            # outputs to the console the unreleased studies
            query_output_incomplete_studies(conn_plsql, published_studies_non_released)
            # close connection
            conn_plsql.close()
            logger.info("Process ended")
        else:
            logger.debug("An issue occured!")
    except psycopg2.DatabaseError as e:
        logger.warning("Error creating database:{} ".format(e))
        raise RuntimeError('Database error') from e
    finally:
        if conn_plsql:
            conn_plsql.close()
            logger.debug("PLSQL connection closed")


""" MAIN """


def main():
    try:
        # configure logging
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s [in %(pathname)s:%(lineno)d]'
        logging.basicConfig(format=log_format)
        # execute main function
        incomplete_submissions()
    except Exception as e:
        logger.error("Error: {}".format(e))
        sys.exit(-1)


if __name__ == "__main__":
    main()
