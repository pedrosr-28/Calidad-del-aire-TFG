            if ciudad == 'Cartagena':
                label_x = ['alumbres_valle_escombreras','la_aljorra','mompean_cartagena']
                label_y = ['valle_escombreras']
            elif ciudad == 'Murcia':
                label_x = [ 'molina_de_segura','mompean_cartagena','caravaca_norte','valle_escombreras','aljorra_litoral','alcantarilla_murcia_ciudad','ronda_sur','lorca_zona_centro',]
                label_y = ['alumbres_valle_escombreras','jumilla','san_basilio_murcia_ciudad']
            elif ciudad == 'Madrid':
                label_x = [ 'algete', 'arturo_soria', 'barrio_del_pilar',
                            'casa_de_campo', 'collado_villalba',
                            'el_pardo', 'ensanche_de_vallecas', 'escuelas_aguirre', 'farolillo',
                            'media_red', 'tres_olivos',
                            'valdemoro', 'villarejo_de_salvanes', 'villaverde', 'plaza_de_castilla',
                            'plaza_de_españa', 'puente_de_vallecas', 
                            'sanchinarro', 'torrejon_de_ardoz']
                label_y = ['mendez_alvaro','alcorcón', 'latina','moratalaz','rivas_vaciamadrid','cuatro_caminos','plaza_elíptica','castellana']
            elif ciudad == 'Sevilla':
                label_x = ['aljarafe', 'bermejales', 'copper_las_cruces', 'ranilla', 'santa_clara', 'the_rosales']
                label_y = ['alcala_de_guadaira', 'principes', 'torneo']
            elif ciudad == 'London':
                label_x = ['bexley_belvedere','brent_ikea','brent_neasden_lane','bexley_belvedere_west',
                           'bloomsbury','coopers_lane',
                           'euston_road','farringdon_street','greenwich_a206_burrage_grove',
                           'greenwich_falconwood_fdms','greenwich_john_harrison_way','greenwich_plumstead_high_street',
                           'greenwich_trafalgar_road_(hoskins_st)','greenwich_westhorne_avenue','greenwich_woolwich_flyover',
                           'hackney_old_street','haringey_roadside','havering_rainham','hillingdon_harmondsworth_os',
                           'horseferry_road','hounslow_brentford','hounslow_chiswick', 'lewisham_honor_oak_park',
                           'lewisham_new_cross','london_haringey_priory_park_south','london_harlington','marylebone_road','n._kensington',
                           'redbridge_ley_street','regent_street_(the_crown_estate)','richmond_upon_thames_barnes_wetlands','richmond_upon_thames_richmond',
                           'sir_john_cass_school','southend_on_sea_uka00409','stanford_le_hope_roadside','surrey_quays_road',
                           'thurrock', 'haringey_haringey_town_hall',
                           'waterloo_place_(the_crown_estate)',
                           'westminster_covent_garden']
                label_y = ['streatham_green','brent_john_keble_primary_school','greenwich_fiveways_sidcup_rd_a20','london_eltham',
                           'tower_hamlets_millwall_park','bexley_slade_green','brent_ark_franklin_primary_academy',
                           'redbridge_gardner_close','london_hillingdon','kerbside',
                           'waltham_forest_dawlish_rd','richmond_upon_thames_castelnau','harrow_stanmore',
                           'tower_hamlets_blackwall','london_teddington_bushy_park']
            elif ciudad == 'Cali':
                label_x = ['calle_3', 'base_aérea_acuaparque', 'compartir', 'calle_33h', 'avenida_2a_norte', 'la_flora', 'carrera_5_norte', 
                           'era_obrero', 'calle_70_norte', 'carrera_83c', 'calle_3_oeste', 'transitoria', 'cañaveralejo', 'la_ermita', 'calle_65_norte',
                           'manuel_m._buenaventura', 'comuna_16', 'el_poblado_ii', 'puente_comercio', 'comuna_10', 'menga', 'bajo_jordan','las_orquideas' , 'comuna_17', 'miraflores', 'barrio_pance']
                label_y = ['calle_18', 'pance', 'calle_3d', 'calle_13d', 'univalle', 'normandia_sebastian_de_belalcazar', 'los_libertadores', 'prados_del_sur', 'santa_fe', 'san_judas_tadeo']
            elif ciudad == 'Bogota':
                label_x = ['tunal', 'calle_86', 'guaymaral', 'fontibon', 'movil_7ma', 'centro_de_alto_rendimiento', 'calle_79a', 
                           'carrera_98', 'las_ferias', 'suba', 'us_consulate', 'kennedy', 'girardot', 'usme', 'gustavo_restrepo', 
                           'buenos_aires', 'santa_lucia', 'sauzalito', 'restrepo', 'laguneta', 'salazar_gomez', 'marichuela', 'sosiego', 
                           'juan_rey', 'rio_negro', 'prado_veraniego', 'sierra_morena_ii_sector', 'nuevo_muzu', 'fatima', 'san_carlos', 'puerta_al_llano_de_usme',
                            'quiroga_central', 'santafe', 'tintala', 'las_colinas', 'la_colmena', 'ciudad_tunal', 'prado_jardin', 'santa_maria_del_lago', 'betania', 
                            'san_eusebio', 'altamar', 'pio_xii', 'zona_8', 'bonanza', 'brasilia', 'el_carmen', 'gran_yosama', 'penon_del_cortijo', 'ricaurte', 'bosa', 'el_palmar'
                            , 'san_jose_oriental', 'perpetuo_socorro', 'gorgonzola', 'eduardo_frey', 'calandaima', 'tenerife', 'tunjuelito', 'las_ferias_occidental'
                            , 'lagos_de_suba', 'engativa', 'jorge_gaitan_cortes', 'galicia', 'bosque_de_maria', 'nuevo_campin', 'niza', 'la_gran_via'
                            , 'carimagua', 'potrerito', 'santa_fe']
                label_y = ['puente_aranda', 'usaquen', 'carvajal', 'san_cristobal', 'minambiente', 'bosque_de_hayuelos', 
                           'villa_beatriz', 'garces_navas', 'la_asuncion', 'ciudad_bolivar', 'san_martin', 'la_estradilla', 'san_benito',
                            'la_isla_del_sol', 'quirigua', 'eduardo_santos', 'san_bernardino_xvi', 'los_campos', 'palestina', 'gibraltar', 'la_giralda',
                            'san_isidro', 'tairona', 'maria_paz', 'julio_flores', 'rincon_del_salitre', 'andalucia', 'barrio_ciudad_universitaria', 
                            'carvajal_sevillana']
            elif ciudad == 'Medellin':
                label_x = ['altavista', 'aranjuez', 'barbosa', 'belén', 'caldas', 'calle_28', 'calle_42', 'calle_65', 'carrera_36b', 'colegio_concejo_de_itagui',
                            'copacabana', 'la_sallista_corporation', 'mobile_station_1', 'mobile_station_1_star_metro', 'museo_de_antioquia', 
                            'politecnico_colombiano_jaime_isaza_cadavid', 'san_cristobal', 'santa_elena']
                label_y = ['casa_de_justicia_itagui', 'envigado', 'bello', 'sabaneta', 'el_poblado', 'the_star', 'tanques_la_ye_medellín', 'villahermosa', 'centro']
