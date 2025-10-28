import pandas as pd

def check_start_end(df):
	for i in range(len(df['Start_frame'])-1):
		if (df['Start_frame'][i] > df['Start_frame'][i+1]) and (df['video_name'][i] == df['video_name'][i+1]):
			print(f"Frames info is not valid at index {i} in the csv sheet")
		if df['Start_frame'][i] == df['End_frame'][i]:
			print(f"Start Frame and end Frame has same number as the frame number at index {i}")


def validate_df(df):
	check_start_end(df)

if __name__=="__main__":
	file="" #pass file path here
	df=pd.read_csv(file)
	validate_df(df)