#!/usr/bin/perl
#
#  1000genomes_output_json.pl - writing 1000 Genomes Project metadata from MySQL
#                               as JSON files
#                 - EOB - Mar 19 2019
#
# usage: 1000genomes_output_json.pl $home_directory
#
########################################

use DBI;
use DBD::mysql;

# connect to local database

my $dsn = 'dbi:mysql:genomes_metadata:localhost:3306';
my $user = 'emmet';
my $password = 'neuro';
my $dbh = DBI->connect($dsn, $user, $password) or die ("Can't connect to database");

# initialise project-level constants

# and remember to spell organisation with a 'z' in this context !

$home_directory       = $ARGV[0];
$species_name         = "Homo sapiens";
$species_id           = "9606";   # NCBI taxonomic identifier for H. sapiens
$species_URL          = "https://www.ncbi.nlm.nih.gov/taxonomy/$species_id";
$project_name         = "1000 Genomes Project";
$project_abbr         = "1KGP";
$publication_doi      = "https://doi.org/10.1038/nature15393";
$publication_title    = "A global reference for human genetic variation";
$publication_date     = "2015-10-01 00:00:00";
$distribution_URL     = "https://datahub-khvul4ng.udes.genap.ca";
$master_filename      = "$home_directory/1000genomes_data/".$project_abbr."_master.json";

# retrieve each row from database and write dataset-level JSON
# for now we are selecting only the columns that contain data in the 1KGP dataset

@dataset_id_array = ();
@dataset_name_array = ();
$dataset_array_count = 0;

$sql_retrieve_genome  = "SELECT chromosome, project_date, resource_link,";
$sql_retrieve_genome .=       " reference_sequence_link, number_of_SNPs, number_of_indels";
$sql_retrieve_genome .=  " FROM 1000genomes ORDER BY chromosome";  
$exec_select = $dbh->prepare($sql_retrieve_genome);
$exec_select->execute();
while (@row = $exec_select->fetchrow_array) {
	write_dataset(@row);
	$chromosome = $row[0];
	$dataset_id_array[$dataset_array_count]   = $project_abbr."_".$chromosome.".json";
	$dataset_name_array[$dataset_array_count] = $chr_name;
	++$dataset_array_count;
}


# write master dataset 

write_master();

exit();

# functions

# write_dataset : write a dataset JSON file for a 1000 Genomes chromosome file 

sub write_dataset {
	
	my ($chromosome, $date, $resource_link, $refseq_link, $SNP_count, $indel_count) = @_;
	
	$dataset_identifier = $project_abbr."_".$chromosome;

	$dataset_filename = "$home_directory/1000genomes_data/".$dataset_identifier.".json";

	my $dataset_text = "{\n";

	open (DATASET_JSON, ">$dataset_filename")|| die "Cannot open $dataset_filename for write\n";

	# generate a human-friendly name from the chromosome ID

	$chromosome =~/chr(.*)/;
	$chr_significant = $1;
	if ($chr_significant eq 'MT') {
		$chr_name = "Mitochondrial genome";
	}
	else {
		$chr_name = "Chromosome $chr_significant";
	}
	
	# version and licensing

	$dataset_text .= "\t\"version\": \"1.0\",\n";
	$dataset_text .= "\t\"privacy\": \"public open\",\n";
	$dataset_text .= "\t\"licenses\": [\n";
	$dataset_text .= "\t\t{\n";
	$dataset_text .= "\t\t\t\"name\": \"to be determined\"\n";
	$dataset_text .= "\t\t}\n";
	$dataset_text .= "\t],\n";

	# write dataset identifier

	$dataset_text .= "\t\"identifier\": {\n";
	$dataset_text .= "\t\t\"identifier\": \"$dataset_identifier\"\n";
	$dataset_text .= "\t},\n";

	# title

	$dataset_text .= "\t\"title\":\"$chr_name\",\n";

	# date

	$dataset_text .= "\t\"dates\": [\n";
	$dataset_text .= "\t\t{\n";
	$dataset_text .= "\t\t\t\"type\": {\n";
	$dataset_text .= "\t\t\t\t\"value\":\"source .vcf file creation date\"\n";
	$dataset_text .= "\t\t\t},\n";
	$dataset_text .= "\t\t\t\"date\": \"$date\"\n";
	$dataset_text .= "\t\t}\n";
	$dataset_text .= "\t],\n";
	
	# data type

	$dataset_text .= "\t\"types\": [\n";
	$dataset_text .= "\t\t{\n";
	$dataset_text .= "\t\t\t\"information\": {\n";
	$dataset_text .= "\t\t\t\t\"value\": \"genomics\"\n";
	$dataset_text .= "\t\t\t}\n";
	$dataset_text .= "\t\t}\n";
	$dataset_text .= "\t],\n";

	# reference links

	$dataset_text .= "\t\"storedIn\": {\n";
	$dataset_text .= "\t\t\"name\": \"$resource_link\",\n";
	$dataset_text .= "\t\t\"description\": \"Gzipped .vcf file containing sequence variations\"\n";
	$dataset_text .= "\t},\n";

	# dimensions

	$dataset_text .= "\t\"dimensions\": [\n";
	$dataset_text .= "\t\t{\n"; 
	$dataset_text .= "\t\t\t\"name\": {\n";
	$dataset_text .= "\t\t\t\t\"value\": \"Count of Single Nucleotide Polymorphism variants\"\n";
	$dataset_text .= "\t\t\t},\n";
	$dataset_text .= "\t\t\t\"values\": [\n";
	$dataset_text .= "\t\t\t\t\"$SNP_count\"\n";
	$dataset_text .= "\t\t\t]\n";
	$dataset_text .= "\t\t},\n"; 
	$dataset_text .= "\t\t{\n"; 
	$dataset_text .= "\t\t\t\"name\": {\n";
	$dataset_text .= "\t\t\t\t\"value\": \"Count of single-nucleotide insertion and deletion events\"\n";
	$dataset_text .= "\t\t\t},\n";
	$dataset_text .= "\t\t\t\"values\": [\n";
	$dataset_text .= "\t\t\t\t\"$indel_count\"\n";
	$dataset_text .= "\t\t\t]\n";
	$dataset_text .= "\t\t}\n";
	$dataset_text .= "\t],\n";

	# creators

	$dataset_text .= "\t\"creators\": [\n";
	$dataset_text .= "\t\t{\n";
	$dataset_text .= "\t\t\t\"name\": \"$project_name\"\n";
	$dataset_text .= "\t\t}\n";
	$dataset_text .= "\t],\n";

	# and add the link to the ftp file containing the reference sequence as an extra property

	$dataset_text .= "\t\"extraProperties\": [\n";
	$dataset_text .= "\t\t{\n";
	$dataset_text .= "\t\t\t\"category\": \"FTP link to gzipped FASTA file containing reference DNA sequence\",\n";
	$dataset_text .= "\t\t\t\"values\": [\n";
	$dataset_text .= "\t\t\t\t{\n";
	$dataset_text .= "\t\t\t\t\t\"value\": \"$refseq_link\"\n";
	$dataset_text .= "\t\t\t\t}\n";
	$dataset_text .= "\t\t\t]\n";
	$dataset_text .= "\t\t}\n";
	$dataset_text .= "\t]\n";

	$dataset_text .= "}\n"; 
	print DATASET_JSON $dataset_text;
	close DATASET_JSON;

}

# write_master: write a master JSON file for the whole dataset

sub write_master {
	
	my $master_text = "{\n";

	open (MAST_JSON, ">$master_filename")|| die "Cannot open $master_filename for write\n";

	$master_text .= "\t\"version\": \"1.0\",\n";
	$master_text .= "\t\"privacy\": \"public open\",\n";
	$master_text .= "\t\"licenses\": [\n";
	$master_text .= "\t\t{\n";
	$master_text .= "\t\t\t\"name\": \"to be determined\"\n";
	$master_text .= "\t\t}\n";
	$master_text .= "\t],\n";

	# identifier
	
	$master_text .= "\t\"identifier\": {\n";
	$master_text .= "\t\t\"identifier\" : \"$project_abbr\"\n";
	$master_text .= "\t},\n";

	$master_text .= "\t\"creators\": [\n";
	$master_text .= "\t\t{\n";
	$master_text .= "\t\t\t\"name\": \"$project_name\"\n";
	$master_text .= "\t\t}\n";
	$master_text .= "\t],\n";

	# data type

	$master_text .= "\t\"types\": [\n";
	$master_text .= "\t\t{\n";
	$master_text .= "\t\t\t\"information\": {\n";
	$master_text .= "\t\t\t\t\"value\": \"genomics\"\n";
	$master_text .= "\t\t\t}\n";
	$master_text .= "\t\t}\n";
	$master_text .= "\t],\n";

	# title

	$master_text .= "\t\"title\": \"$project_name\",\n";

	# location of resources

	$master_text .= "\t\"storedIn\": {\n";
	$master_text .= "\t\t\"name\" : \"Canadian Centre for Computational Genomics\"\n";
	$master_text .= "\t},\n";

	# publication info

	$master_text .= "\t\"primaryPublications\" : [\n";
	$master_text .= "\t\t{\n";
	$master_text .= "\t\t\t\"identifier\": {\n";
	$master_text .= "\t\t\t\t\"identifier\": \"$publication_doi\"\n";
	$master_text .= "\t\t\t},\n";
	$master_text .= "\t\t\t\"title\": \"$publication_title\",\n";

	$master_text .= "\t\t\t\"dates\": [\n";
	$master_text .= "\t\t\t\t{\n";
	$master_text .= "\t\t\t\t\t\"type\": {\n";
	$master_text .= "\t\t\t\t\t\t\"value\":\"Primary reference publication date\"\n";
	$master_text .= "\t\t\t\t\t},\n",
	$master_text .= "\t\t\t\t\t\"date\": \"$publication_date\"\n";
	$master_text .= "\t\t\t\t}\n";
	$master_text .= "\t\t\t],\n";

	$master_text .= "\t\t\t\"authors\": [\n";
	$master_text .= "\t\t\t\t{\n";
	$master_text .= "\t\t\t\t\"name\":\"$project_name\"\n";
	$master_text .= "\t\t\t\t}\n";
	$master_text .= "\t\t\t]\n";
	$master_text .= "\t\t}\n";
	$master_text .= "\t],\n";

	# isAbout; setting this to taxonomic information for the moment

	$master_text .= "\t\"isAbout\": [\n";
	$master_text .= "\t\t{\n";
	$master_text .= "\t\t\t\"identifier\": {\n";
	$master_text .= "\t\t\t\t\"identifier\": \"$species_id\",\n";
	$master_text .= "\t\t\t\t\"identifierSource\":\"$species_URL\"\n";
	$master_text .= "\t\t\t},\n";
	$master_text .= "\t\t\t\"name\":\"$species_name\"\n";
	$master_text .= "\t\t}\n";
	$master_text .= "\t],\n";

	# JSON fileset creation date

	$master_text .= "\t\"dates\": [\n";
	$master_text .= "\t\t{\n";
	$master_text .= "\t\t\t\"type\": {\n";
	$master_text .= "\t\t\t\t\"value\":\"CONP DATS JSON fileset creation date\"\n";
	$master_text .= "\t\t\t},\n",
	@date_now     = localtime(); # reformat this to a date format JSON likes:
	$date_out     = ($date_now[5]+1900)."-".sprintf("%02d",$date_now[4]+1);
	$date_out    .=  "-".sprintf("%02d",$date_now[3])." ";  # YYYY-MM-DD
	$date_out    .= sprintf("%02d",$date_now[2]).":";
	$date_out    .= sprintf("%02d",$date_now[1]).":";
	$date_out    .= sprintf("%02d",$date_now[0]);      # hh:mm:ss
	$master_text .= "\t\t\t\"date\": \"$date_out\"\n";
	$master_text .= "\t\t}\n";
	$master_text .= "\t],\n";
	
	# hasPart: list subdatasets of this one, with required fields

	$master_text .= "\t\"hasPart\": [\n";
	$temp_counter = 0;
	while ($temp_counter < ($dataset_array_count)) {
		$master_text .= "\t\t{\n";

		# identifier
		
		$master_text .= "\t\t\t\"identifier\": {\n";
		$master_text .= "\t\t\t\t\"identifier\": \"$dataset_id_array[$temp_counter]\"\n";
		$master_text .= "\t\t\t},\n";

		# title

		$master_text .= "\t\t\t\"title\":\"$dataset_name_array[$temp_counter]\",\n";


		# creators

		$master_text .= "\t\t\t\"creators\": [\n";
		$master_text .= "\t\t\t\t{\n";
		$master_text .= "\t\t\t\t\t\"name\": \"$project_name\"\n";
		$master_text .= "\t\t\t\t}\n";
		$master_text .= "\t\t\t],\n";

		# data type

		$master_text .= "\t\t\t\"types\": [\n";
		$master_text .= "\t\t\t\t{\n";
		$master_text .= "\t\t\t\t\t\"information\": {\n";
		$master_text .= "\t\t\t\t\t\t\"value\": \"genomics\"\n";
		$master_text .= "\t\t\t\t\t}\n";
		$master_text .= "\t\t\t\t}\n";
		$master_text .= "\t\t\t]\n";
		
		$master_text .= "\t\t}";
		unless ($temp_counter == $dataset_array_count - 1) {
			$master_text .= ",";  # comma needed after every entry except the last
		}
		$master_text .= "\n";

		++$temp_counter;
	}

	$master_text .= "\t]\n";

	$master_text .= "}\n";
	print MAST_JSON $master_text;
	close MAST_JSON;

}


