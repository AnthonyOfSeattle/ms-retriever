def get_conversion_input(wildcards):
    """Get relative or absolute location of input"""

    record = samples.set_index(["dataset", "sample_name"]).T[wildcards.dataset][wildcards.sample_name]
    return os.path.join(record.input_location, record.input_file)

def get_converted_format(wildcards):
    """Get integer code corresponding to desired conversion output format"""

    return {"mgf": 0, "mzML": 2}[wildcards.extension]

def get_metadata_format(wildcards):
    """Get integer code corresponding to desired metadata output format"""

    assert config["conversion"]["meta_format"] in ["json", "txt"]
    return {"json": 0, "txt": 1}[config["conversion"]["meta_format"]]

def get_metadata_output_file(wildcards):
    """Get filename for metadata"""

    meta_file_name = wildcards.sample_name + ".meta." + config["conversion"]["meta_format"]
    return os.path.join(config["output"], "samples", wildcards.dataset, meta_file_name)

rule convert_raw:
    input:
        get_conversion_input
    output:
        os.path.join(config["output"], 
                     "samples", 
                     "{dataset}", 
                     "{sample_name}.{extension,(mzML)|(mgf)}")
    conda:
        "../envs/conversion.yaml"
    params:
        format = get_converted_format,
        metadata = get_metadata_format,
        metadata_output_file = get_metadata_output_file
    shell:
        """
        ThermoRawFileParser.sh --input={input} \
                               --output_file={output} \
                               --format={params.format} \
                               --metadata={params.metadata} \
                               --metadata_output_file={params.metadata_output_file}
        """
