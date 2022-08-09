library(data.table)
library(logger)
library(rjson)
library(sdcMicro)


url <- "https://raw.githubusercontent.com/globaldothealth/monkeypox/main/latest.csv"
cfgFile <- "config.json"
dataFile <- "/output/sdcmicro.csv"
outputDir <- "/output"
reportFile <- "sdcmicro"

log_info("Reading from URL")
data <- fread(url)
log_info("Reading from config file")
config <- fromJSON(file=cfgFile)
log_info("Done reading files")

# Remove (Y/N/NA) to avoid one error...escaping lead to other errors...
setnames(data, "Hospitalised (Y/N/NA)", "Hospitalised")

selectedKeyVars <- unlist(config["key_variables"])

log_info("Converting variables into factors")
cols = selectedKeyVars
data[,cols] <- lapply(data[, ..cols], factor)

log_info("Converting the sub file into dataframe")
subVars <- c(selectedKeyVars)
fileRes <- data[, ..subVars]
fileRes <- as.data.frame(fileRes)

log_info("Creating SDC object")
sdc <- createSdcObj(dat = fileRes, 
                    keyVars = selectedKeyVars)

log_info("Finding re-identification risk")
risk <- sdc@risk
originalRisk <- sdc@originalRisk

iR_mod <- risk$individual[,1]
s_mod <-sum((iR_mod > median(iR_mod) + 2*mad(iR_mod)) & (iR_mod>0.1))

exp <- round(risk$global$risk_ER, 2)
exp_pct <- round(risk$global$risk_pct, 2)

log_info("Saving risk data")
df <- data.frame(
    HigherRiskObservations = c(s_mod),
    ExpectedReIdentifications = c(exp),
    PercentExpectedReId = c(exp_pct)
)

write.csv(df, dataFile, row.names=FALSE)

log_info("Saving additional report")
report(sdc, outdir=outputDir, filename=reportFile, internal=T, verbose=TRUE)
