/**************************************************************************/
/*                                                                        */
/*      Copyright (c) 2005-2015 by ITTIA L.L.C. All rights reserved.      */
/*                                                                        */
/*  This software is copyrighted by and is the sole property of ITTIA     */
/*  L.L.C.  All rights, title, ownership, or other interests in the       */
/*  software remain the property of ITTIA L.L.C.  This software may only  */
/*  be used in accordance with the corresponding license agreement.  Any  */
/*  unauthorized use, duplication, transmission, distribution, or         */
/*  disclosure of this software is expressly forbidden.                   */
/*                                                                        */
/*  This Copyright notice may not be removed or modified without prior    */
/*  written consent of ITTIA L.L.C.                                       */
/*                                                                        */
/*  ITTIA L.L.C. reserves the right to modify this software without       */
/*  notice.                                                               */
/*                                                                        */
/*  info@ittia.com                                                        */
/*  http://www.ittia.com                                                  */
/*                                                                        */
/*                                                                        */
/**************************************************************************/

#include "storage/data/environment.h"
#include <iostream>
#include <stdlib.h>

int example_main(int argc, char* argv[]);

int db_main(int argc, char* argv[])
{
    int exit_code;
    int status;

    /* Initialize ITTIA DB SQL library. */
    status = storage::data::Environment::init();

    if (DB_SUCCESS(status)) {
        db_api_statistics_t api_stats;
        db_lm_statistics_t lm_stats;

#ifdef _DEBUG
        /* Enable collection of statistics. */
        storage::data::Environment::get_api_statistics(NULL, DB_STATISTICS_ENABLE);
        storage::data::Environment::get_lm_statistics(NULL, DB_STATISTICS_ENABLE);
#endif

        /* Run the example application. */
        exit_code = example_main(argc, argv);

        /* Release all resources owned by the ITTIA DB SQL library. */
        status = storage::data::Environment::done(NULL, &api_stats, &lm_stats);

        if (DB_SUCCESS(status)) {
            if (api_stats.have_statistics) {
                std::cout << "ITTIA DB SQL final C API resource statistics:\n";
                std::cout << " db:      " << api_stats.db.cur_value      << " remain open of " << api_stats.db.max_value        << " max" << std::endl;
                std::cout << " row:     " << api_stats.row.cur_value     << " remain open of " << api_stats.row.max_value       << " max" << std::endl;
                std::cout << " cursor:  " << api_stats.cursor.cur_value  << " remain open of " << api_stats.cursor.max_value    << " max" << std::endl;
                std::cout << " seq:     " << api_stats.seq.cur_value     << " remain open of " << api_stats.seq.max_value       << " max" << std::endl;
                std::cout << " seqdef:  " << api_stats.seqdef.cur_value  << " remain open of " << api_stats.seqdef.max_value    << " max" << std::endl;
                std::cout << " tabledef:" << api_stats.tabledef.cur_value<< " remain open of " << api_stats.tabledef.max_value  << " max" << std::endl;
                std::cout << " indexdef:" << api_stats.indexdef.cur_value<< " remain open of " << api_stats.indexdef.max_value  << " max" << std::endl;
                std::cout << " oid:     " << api_stats.oid.cur_value     << " remain open of " << api_stats.oid.max_value       << " max" << std::endl;
            }

            if (lm_stats.have_statistics) {
                std::cout << "ITTIA DB SQL final lock manager resource statistics: " <<std::endl;
                std::cout << " nlocks:   " << lm_stats.nlocks.cur_value   << " remain open of " << lm_stats.nlocks.max_value  << " max" << std::endl;
                std::cout << " nowners:  " << lm_stats.nowners.cur_value  << " remain open of " << lm_stats.nowners.max_value << " max" << std::endl;
                std::cout << " nobjects: " << lm_stats.nobjects.cur_value << " remain open of " << lm_stats.nobjects.max_value << " max" << std::endl;
            }
        }
    }

    if (DB_FAILED(status)) {
        std::cerr << "ITTIA DB SQL init/done error: "<< status << std::endl;
        exit_code = EXIT_FAILURE;
    }

    return exit_code;
}
