import pandas as pd

class SampleFile:
    def __init__(self):
        pass
    
    def sampler(self, input_file: str, output_file: str, sample_size: int = 100):
        """
        Samples 100 rows from the input parquet file and saves the sample to the output parquet file.

        Parameters:
        input_file (str): Path to the input parquet file.
        output_file (str): Path to save the output parquet file.
        sample_size (int): Number of rows to sample from the input file. Defaults to 100.
        """

        try:
            df_input = pd.read_parquet(input_file)

            if sample_size > len(df_input):
                raise ValueError("Sample size exceeds the number of rows in the input file")\
                
            df_sample = df_input.sample(n=sample_size, random_state=1)

            temp_csv_file = "temp_sample.csv"
            df_sample.to_csv(temp_csv_file, sep=';', index=False)
                
            df_sample_with_delimiter = pd.read_csv(temp_csv_file, sep=';')
            df_sample_with_delimiter.to_parquet(output_file, engine='fastparquet', index=False)

            print(f"Sample data has been saved to {output_file}")

        except Exception as e:
            print(f"Error occurred: {e}")