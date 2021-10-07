drop table gathering_site;
drop table responsible;
drop table gathering;
drop table site;
drop table gathering_responsible;
drop table specimen;
drop table taxon_tree;
drop table kingdom;
drop table pylum_division;
drop table clase;
drop table orden;
drop table familia;
drop table genus;
drop table taxon;




delete from gathering_site;
delete from responsible;
delete from gathering;
delete from site;
delete from gathering_responsible;
delete from specimen;
delete from taxon_tree;
delete from kingdom;
delete from pylum_division;
delete from clase;
delete from orden;
delete from familia;
delete from genus;
delete from taxon;


select * from taxon_tree;
delete from ejemplo;

create table kingdom(
	kingdom_id serial primary key, 
	kingdom_name varchar(200)
);

create table pylum_division(
	pylum_division_id serial primary key, 
	pylum_division_name varchar(200)
);

create table clase(
	clase_id  serial primary key, 
	clase_name varchar(200)
);

create table orden(
	orden_id  serial primary key, 
	orden_name varchar(200)
);

create table familia (
	familia_id  serial primary key, 
	familia_name varchar(200)
);

create table genus(
	genus_id  serial primary key, 
	genus_name varchar(200)
);

create table taxon(
    taxon_id  serial primary key,
    species_name varchar(300),
    scientific_name varchar(300)
);

create table taxon_tree(
    taxon_tree_id int primary key,
    taxon_id int,
    kingdom_id  int,
    pylum_division_id int,
    clase_id int,
    orden_id int,
    familia_id int,
    genus_id int,
    CONSTRAINT fk_taxon_tree FOREIGN KEY(taxon_id) REFERENCES taxon(taxon_id),
    CONSTRAINT fk_kingdom_tree FOREIGN KEY(kingdom_id) REFERENCES kingdom(kingdom_id),
    CONSTRAINT fk_pylum_division_tree FOREIGN KEY(pylum_division_id) REFERENCES pylum_division(pylum_division_id),
    CONSTRAINT fk_clase_tree FOREIGN KEY(clase_id) REFERENCES clase(clase_id),
    CONSTRAINT fk_orden_tree FOREIGN KEY(orden_id) REFERENCES orden(orden_id),
    CONSTRAINT fk_familia_tree FOREIGN KEY(familia_id) REFERENCES familia(familia_id),
    CONSTRAINT fk_genus_tree FOREIGN KEY(genus_id) REFERENCES genus(genus_id)
);

create table specimen(
	specimen_id int primary key,
	specimen_description text,
	specimen_cost float not null,
	taxon_tree_id int,
	CONSTRAINT fk_taxon_tree_id FOREIGN KEY(taxon_tree_id) REFERENCES taxon_tree(taxon_tree_id)
);

create table gathering_responsible(
 	gathering_responsible_id  serial primary key,
	name varchar(50) not null
);

create table site(
 	site_id int primary key,
	latitude float not null,
	longitude float not null,
	site_descripcion text not null
);

create table gathering(
 	gathering_id  serial primary key,
 	specimen_id int,
	gathering_date date not null,
 	CONSTRAINT fk_specimen_gathering FOREIGN KEY(specimen_id) REFERENCES specimen(specimen_id)
);

create table responsible(
 	gathering_id int,
	gathering_responsible_id int,
	constraint fk_gathering foreign key (gathering_id) REFERENCES gathering(gathering_id),
	constraint fk_site foreign key (gathering_responsible_id) REFERENCES gathering_responsible(gathering_responsible_id)
);

create table gathering_site(
 	gathering_id int,
	site_id int,
	constraint fk_gathering foreign key (gathering_id) REFERENCES gathering(gathering_id),
	constraint fk_site foreign key (gathering_id) REFERENCES gathering(gathering_id)
);


create table datos(
linea text 
);


delete from datos;
COPY datos FROM '/Users/PC_ESQUIVEL_LOPEZ/Desktop/200000.csv';

select * from datos;

create or replace procedure normalizar()
language plpgsql
as $$
declare
linea VARCHAR(4000);
specimen_id int;
taxon_id int;
gathering_date date;
kingdom_name varchar(200);
phylum_name varchar(200);
clase_name varchar(200);
orden_name varchar(200);
family_name varchar(200);
genus_name varchar(200);
specie_name varchar(200);
scientific_name varchar(200);
responsible_names varchar(300);
site_id int;
latitud float;
longitud float;
site_description varchar(3000);
specimen_description varchar(3000);
specimen_cost float;
ver_taxon integer;
ver_site integer;
ver_responsible integer;
ver_specimen integer;
ver_gathering integer;
filas record;
begin
for filas in (SELECT * FROM datos)
    loop
        linea := trim(filas.linea);-- se seleccionan todos los departamentos de x fila de la tabla temporal
        if length(linea) >= 0 then
               	specimen_id := to_number(substr(linea, 1, position('|' in linea)-1),'999999999');
                linea := trim(substr(linea, position('|' in linea)+1));
				taxon_id := to_number(substr(linea, 1, position('|' in linea)-1),'999999999');
                linea := trim(substr(linea, position('|' in linea)+1));
				gathering_date := to_date(substr(linea, 1, position('|' in linea)-1),'DD-MM-YYYY');
                linea := trim(substr(linea, position('|' in linea)+1));
				kingdom_name := substr(linea, 1, position('|' in linea)-1);
                linea := trim(substr(linea, position('|' in linea)+1));
				phylum_name := substr(linea, 1, position('|' in linea)-1);
                linea := trim(substr(linea, position('|' in linea)+1));
				clase_name := substr(linea, 1, position('|' in linea)-1);
                linea := trim(substr(linea, position('|' in linea)+1));
				orden_name := substr(linea, 1, position('|' in linea)-1);
                linea := trim(substr(linea, position('|' in linea)+1));
				family_name := substr(linea, 1, position('|' in linea)-1);
                linea := trim(substr(linea, position('|' in linea)+1));
				genus_name := substr(linea, 1, position('|' in linea)-1);
                linea := trim(substr(linea, position('|' in linea)+1));
				specie_name := substr(linea, 1, position('|' in linea)-1);
                linea := trim(substr(linea, position('|' in linea)+1));
				scientific_name := substr(linea, 1, position('|' in linea)-1);
                linea := trim(substr(linea, position('|' in linea)+1));
				responsible_names := substr(linea, 1, position('|' in linea)-1);
                linea := trim(substr(linea, position('|' in linea)+1));
				site_id := to_number(substr(linea, 1, position('|' in linea)-1),'999999');
                linea := trim(substr(linea, position('|' in linea)+1));
				latitud := to_number(substr(linea, 1, position('|' in linea)-1),'S99.999999999');
                linea := trim(substr(linea, position('|' in linea)+1));
				longitud := to_number(substr(linea, 1, position('|' in linea)-1),'S99.999999999');
                linea := trim(substr(linea, position('|' in linea)+1));
				site_description := substr(linea, 1, position('|' in linea)-1);
                linea := trim(substr(linea, position('|' in linea)+1));
				specimen_description := substr(linea, 1, position('|' in linea)-1);
                linea := trim(substr(linea, position('|' in linea)+1));
				specimen_cost := to_number(linea,'9999.99');
				ver_taxon := insertar_taxon(taxon_id,kingdom_name,phylum_name,clase_name,orden_name,family_name,genus_name,specie_name,scientific_name);
				ver_site := insertar_site(site_id,latitud,longitud,site_description);
				ver_responsible := insertar_responsible(responsible_names);
				ver_specimen := insertar_specimen(specimen_id,specimen_description,specimen_cost,ver_taxon);
				ver_gathering := insertar_gathering(ver_specimen,ver_site,ver_responsible,gathering_date);
				--raise notice 'Value: %', ver_gathering;
		end if;
    end loop;
end;
$$

call normalizar();

select * from specimen;

drop function insertar_taxon(id_taxon integer,kingdom_n varchar,phylum_n varchar,clase_n varchar,orden_n varchar,family_n varchar,genus_n varchar,specie varchar,scientific varchar);

create or replace function insertar_taxon(id_taxon integer,kingdom_n varchar,phylum_n varchar,clase_n varchar,orden_n varchar,family_n varchar,genus_n varchar,specie varchar,scientific varchar)
	returns integer 
    language plpgsql
    as $$
    declare
	temporal int;
	taxon_name_id int;
    king_id int;
	phy_id int;
	cla_id int;
	ord_id int;
	fami_id int;
	gen_id int;
    begin
     select taxon_tree_id into temporal from taxon_tree where taxon_tree.taxon_tree_id = id_taxon;	 
	 select taxon_id into taxon_name_id from taxon where taxon.scientific_name = scientific;	 
	 select kingdom_id into king_id from kingdom where kingdom.kingdom_name = kingdom_n;
	 select pylum_division_id into phy_id from pylum_division where pylum_division.pylum_division_name = phylum_n;
	 select clase_id into cla_id from clase where clase.clase_name = clase_n;	 
	 select orden_id into ord_id from orden where orden.orden_name = orden_n;
	 select familia_id into fami_id from familia where familia.familia_name = family_n; 	 
	 select genus_id into gen_id from genus where genus.genus_name = genus_n; 
	 if (taxon_name_id is null ) then
		insert into taxon(species_name, scientific_name) values(specie,scientific);
		select taxon_id into taxon_name_id from taxon where taxon.scientific_name = scientific;
	 end if;
	 if (king_id is null) then
		insert into kingdom(kingdom_name) values(kingdom_n);
		select kingdom_id into king_id from kingdom where kingdom.kingdom_name = kingdom_n;	
	 end if;
	 if (phy_id is null) then
		insert into pylum_division(pylum_division_name) values(phylum_n);
		select pylum_division_id into phy_id from pylum_division where pylum_division.pylum_division_name = phylum_n;
	 end if;
	 if (cla_id is null ) then
		insert into clase(clase_name) values(clase_n);
		select clase_id into cla_id from clase where clase.clase_name = clase_n;
	 end if;
	 if (ord_id is null ) then
		insert into orden(orden_name) values(orden_n);
		select orden_id into ord_id from orden where orden.orden_name = orden_n;
	 end if;
	 if (fami_id is null ) then
		insert into familia(familia_name) values(family_n);
		select familia_id into fami_id from familia where familia.familia_name = family_n;
	 end if;
	 if (gen_id is null ) then
		insert into genus(genus_name) values(genus_n);
		select genus_id into gen_id from genus where genus.genus_name = genus_n;
	 end if; 
	 if (temporal is null) then
	 	insert into taxon_tree(taxon_tree_id,taxon_id,kingdom_id,pylum_division_id,clase_id,orden_id,familia_id,genus_id) values(id_taxon,taxon_name_id,king_id,phy_id,cla_id,ord_id,fami_id,gen_id);
		select taxon_tree_id into temporal from taxon_tree where taxon_tree.taxon_tree_id = id_taxon;
	 	return temporal;
	 else
		return temporal;
	 end if;
			
    end;
$$

drop function insertar_taxon(id_taxon integer,kingdom_n varchar,phylum_n varchar,clase_n varchar,orden_n varchar,family_n varchar,genus_n varchar,specie varchar,scientific varchar);



create or replace function insertar_site(id_site integer, lat float ,long float,site_desc varchar)
	returns integer 
    language plpgsql
    as $$
    declare
	temporal int;
    begin
     select site_id into temporal from site where site.site_id = id_site;	 
	 if (temporal is null) then
	 	insert into site(site_id,latitude,longitude,site_descripcion) values(id_site,lat,long,site_desc);
		select site_id into temporal from site where site.site_id = id_site;
	 	return temporal;
	 else
		return temporal;
	 end if;
			
    end;
$$

create or replace function insertar_responsible(responsible varchar)
	returns integer 
    language plpgsql
    as $$
    declare
	temporal int;
    begin
     select gathering_responsible_id into temporal from gathering_responsible where gathering_responsible.name = responsible;	 
	 if (temporal is null) then
	 	insert into gathering_responsible(name) values(responsible);
		select gathering_responsible_id into temporal from gathering_responsible where gathering_responsible.name = responsible;
	 	return temporal;
	 else
		return temporal;
	 end if;
			
    end;
$$

create or replace function insertar_specimen(spe_id integer,specimen_descr varchar,spec_cost float,ver_taxon integer)
	returns integer 
    language plpgsql
    as $$
    declare
	temporal int;
    begin
     select specimen_id into temporal from specimen where specimen.specimen_id = spe_id;	 
	 if (temporal is null) then
	 	insert into specimen(specimen_id,specimen_description,specimen_cost,taxon_tree_id) values(spe_id,specimen_descr,spec_cost,ver_taxon);
	 	select specimen_id into temporal from specimen where specimen.specimen_id = spe_id;
		return temporal;
	 else
		return temporal;
	 end if;		
    end;
$$

--ver_gathering := insertar_gathering(ver_specimen,ver_site,ver_responsible,gathering_date);
create or replace function insertar_gathering(ver_spec integer,ver_sit integer,ver_responsible integer,gat_date date)
	returns integer 
    language plpgsql
    as $$
    declare
	temporal_gat int;
    begin
     select specimen_id into temporal_gat from gathering where gathering.specimen_id = ver_spec and gathering.gathering_date = gat_date ;	 
	 if (temporal_gat is null) then
	 	insert into gathering(specimen_id,gathering_date) values(ver_spec,gat_date);
		select gathering_id into temporal_gat from gathering where gathering.specimen_id = ver_spec and gathering.gathering_date = gat_date;
		insert into gathering_site(gathering_id,site_id) values(temporal_gat,ver_sit);
		insert into responsible(gathering_id,gathering_responsible_id) values(temporal_gat,ver_responsible); 	 
		return temporal_gat;
	 else
		return temporal_gat;
	 end if;		
    end;
$$

select * from site;
delete from site;
SELECT
    TO_NUMBER(
        '727.64',
        '999999.99'
    );
SELECT
    TO_DATE(
        '11-02-1999',
        'DD-MM-YYYY'
    );

create table taxon_names(
	taxon_id int primary key,
	scientific_name text,
	species_name text,
	kingdom_name text,
	pylum_division_name text,
	clase_name text,
	orden_name text,
	familia_name text,
	genus_name text
);

create table gathering_date(
	gathering_date_id serial primary key,
	year int,
	month int,
	day int
);

create table specimen_fact(
	taxon int,
	site int,
	gathering_date int,
	gathering_responsible int,
	specimen_count int,
	cost_sum float,
	constraint fk_specimen_fact_taxon foreign key (taxon) REFERENCES taxon_names(taxon_id),
	constraint fk_specimen_fact_site foreign key (site) REFERENCES site(site_id),
	constraint fk_specimen_fact_date foreign key (gathering_date) REFERENCES gathering_date(gathering_date_id),
	constraint fk_specimen_fact_responsible foreign key (gathering_responsible) REFERENCES gathering_responsible(gathering_responsible_id)
);


create or replace procedure olap()
language plpgsql
as $$
declare
taxones_id int;
gathering_fecha int;
sitio_id int;
responsible_id int;
ye int;
mon int;
d int;
filas record;
begin
for filas in (select sp.taxon_tree_id as taxon_id,k.kingdom_name as kingdom,p.pylum_division_name as pylum_division,c.clase_name as clase,o.orden_name as orden,f.familia_name as familia,g.genus_name as genus,t.species_name as species,t.scientific_name as scientific,
 si.site_id as site_id, si.latitude as latitud, si.longitude as longitud, si.site_descripcion as description,
 ga.gathering_date as GATHERING_DATE,
 gre.GATHERING_RESPONSIBLE_id as GATHERING_RESPONSIBLE_id, gre.name as GATHERING_RESPONSIBLE,
 COUNT(sp.specimen_id) as specimen_count,
 SUM(sp.specimen_cost ) as specimen_cost
from 
	taxon_tree as tt
	INNER JOIN kingdom as k on k.kingdom_id = tt.kingdom_id
	INNER JOIN pylum_division as p on p.pylum_division_id = tt.pylum_division_id 
	INNER JOIN clase as c on c.clase_id  = tt.clase_id
	INNER JOIN orden as o on o.orden_id  = tt.orden_id
	INNER JOIN familia  as f on f.familia_id   = tt.familia_id 
	INNER JOIN genus as g on g.genus_id   = tt.genus_id  
	INNER JOIN taxon as t on t.taxon_id = tt.taxon_id	
	INNER JOIN specimen as sp on sp.taxon_tree_id   = tt.taxon_tree_id	
	INNER JOIN gathering as ga on ga.specimen_id  = sp.specimen_id 	
	INNER JOIN gathering_site as gs on gs.gathering_id = ga.gathering_id
	INNER JOIN site as si on si.site_id  = gs.site_id
	INNER JOIN responsible as re on re.gathering_id = ga.gathering_id
	INNER JOIN gathering_responsible as gre on gre.gathering_responsible_id = re.gathering_responsible_id
	Group by (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)) loop
		select taxon_id into taxones_id from taxon_names where filas.taxon_id = taxon_names.taxon_id;
		select gathering_date_id into gathering_fecha from gathering_date where extract(year from filas.gathering_date) = gathering_date.year and extract(month from filas.gathering_date) = gathering_date.month and extract(day from filas.gathering_date) = gathering_date.day;
		select site_id into sitio_id from site where filas.site_id = site.site_id;
		select GATHERING_RESPONSIBLE_id into responsible_id from GATHERING_RESPONSIBLE where filas.GATHERING_RESPONSIBLE_id = GATHERING_RESPONSIBLE.GATHERING_RESPONSIBLE_id;
		if (taxones_id is null) then
			insert into taxon_names(taxon_id,scientific_name,species_name,kingdom_name,pylum_division_name,clase_name,orden_name,familia_name,genus_name) values(filas.taxon_id,filas.scientific,filas.species,filas.kingdom,filas.pylum_division,filas.clase,filas.orden,filas.familia,filas.genus);
			taxones_id := filas.taxon_id;
		end if;
		if (gathering_fecha is null) then
			select extract(year from filas.GATHERING_DATE) into ye;
			select extract(month from filas.GATHERING_DATE) into mon;
			select extract(day from filas.GATHERING_DATE) into d;
			insert into gathering_date(year,month,day) values (ye,mon,d);
			select gathering_date_id into gathering_fecha from gathering_date where ye = gathering_date.year and mon = gathering_date.month and d = gathering_date.day;
		end if;
		insert into specimen_fact(taxon,site,gathering_date,gathering_responsible,specimen_count,cost_sum) values(taxones_id,sitio_id,gathering_fecha,responsible_id,filas.specimen_count,filas.specimen_cost);
	end loop;
end;
$$

call olap()

create or replace function numero_especimenes(mes integer, tax text)
	returns int 
    language plpgsql
    as $$
    declare
	temporal int;
    begin 
	select count(*) as cantidad into temporal from taxon_names as tx
	INNER JOIN gathering_date as gd on gd.month = mes
	INNER JOIN specimen_fact as sf on sf.gathering_date = gd.gathering_date_id
	where tx.kingdom_name = tax and sf.taxon = tx.taxon_id or tx.scientific_name = tax and sf.taxon = tx.taxon_id or tx.species_name = tax and sf.taxon = tx.taxon_id or tx.pylum_division_name = tax and sf.taxon = tx.taxon_id or tx.clase_name = tax and sf.taxon = tx.taxon_id  or tx.orden_name = tax and sf.taxon = tx.taxon_id or tx.familia_name = tax  and sf.taxon = tx.taxon_id;
	return temporal;
    end;
$$


DROP FUNCTION numero_especimenes(integer,text)
select numero_especimenes(9,'Plantae')

SELECT gd.year AS YEAR,gd.month AS MONTH, SUM(sf.cost_sum) AS suma, sf.specimen_count AS cantidad FROM gathering_date AS gd
	INNER JOIN specimen_fact as sf on sf.gathering_date = gd.gathering_date_id
	GROUP BY ROLLUP (YEAR,MONTH,cantidad)
	ORDER BY 1,2;
SELECT gd.year AS YEAR,tn.kingdom_name as Kingdom,tn.pylum_division_name as Pylum_division,tn.clase_name as Clase,tn.orden_name as Orden,tn.familia_name as Famila,tn.genus_name as Genus,tn.scientific_name as Scientific, tn.species_name as Species, SUM(sf.cost_sum) AS costo, SUM(sf.specimen_count) AS cantidad FROM gathering_date AS gd
	INNER JOIN specimen_fact as sf on sf.gathering_date = gd.gathering_date_id
	INNER JOIN taxon_names as tn on tn.taxon_id  = sf.taxon 
	GROUP BY CUBE (YEAR,Kingdom,Pylum_division,Clase,Orden,Famila,Genus,Scientific,Species)
