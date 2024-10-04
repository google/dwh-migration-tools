package com.google.edwmigration.dumper.build.licensereport

import org.gradle.api.tasks.Internal

import static com.github.jk1.license.render.LicenseDataCollector.singleModuleLicenseInfo

import org.gradle.api.tasks.Input
import com.github.jk1.license.License
import com.github.jk1.license.LicenseReportExtension
import com.github.jk1.license.ModuleData
import com.github.jk1.license.ProjectData
import com.github.jk1.license.render.ReportRenderer
import org.apache.commons.csv.CSVPrinter
import org.apache.commons.csv.CSVFormat

import java.util.stream.Collectors

/**
 * Custom CSV reporter that gets the license from the POM or the manifest but
 * does not try to deduce it from a LICENSE file.
 *
 * Project overrides can be provided for example to provide license URLs for
 * projects where the license is not declared in a POM or manifest.
 **/
class CsvReportRenderer implements ReportRenderer {
    @Internal
    String[] header = ["artifact", "project url", "module license", "module license url"]
    @Internal
    CSVFormat csvFormat = CSVFormat.EXCEL.builder().setHeader(header).build()

    @Input
    String filename

    CsvReportRenderer(String filename = 'licenses.csv') {
        this.filename = filename
    }

    @Override
    void render(ProjectData projectData) {
        Map<String, Map<String, String>> allOverrides = OverridesUtil.projectOverrides()

        List<Map<String, ?>> records = projectData.allDependencies.sort().stream().map {
            String project = "${it.group}:${it.name}"
            String artifact = "${it.group}:${it.name}:${it.version}"
            Map<String, String> projectInfoOverride = allOverrides.getOrDefault(project, [:])
            License license = getLicense(it)
            String projectUrl = it.poms.find { it.projectUrl }?.projectUrl
            [
                artifact            : artifact,
                "project url"       : projectInfoOverride.getOrDefault("projectUrl", projectUrl),
                "module license"    : projectInfoOverride.getOrDefault("licenseName", license?.name),
                "module license url": projectInfoOverride.getOrDefault("licenseUrl", license?.url),
            ]
        }.collect(Collectors.toList())

        LicenseReportExtension config = projectData.project.licenseReport
        FileWriter writer = new FileWriter(new File(config.outputDir, filename))
        CSVPrinter csv = new CSVPrinter(writer, csvFormat)
        try {
            csv.printRecords(records.stream().map { record -> header.collect { record[it] } })
        } finally {
            csv.close()
        }
    }

    License getLicense(ModuleData moduleData) {
        License license = moduleData.poms.collectMany { it.licenses }.find()
        if (license) {
            return license
        }
        String licenseName = moduleData.manifests.find { it.license }?.license
        String licenseUrl = moduleData.manifests.find { it.licenseUrl }?.licenseUrl
        if (licenseName || licenseUrl) {
            return new License(name: licenseName, url: licenseUrl)
        }
        return null
    }
}