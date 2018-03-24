Embulk::JavaPlugin.register_input(
  "athena", "org.embulk.input.athena.AthenaInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
